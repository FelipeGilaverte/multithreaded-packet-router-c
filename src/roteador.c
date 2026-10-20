/*
 * ================================================================
 *  SISTEMA DE ROTEAMENTO DE PACOTES MULTITHREAD
 *  Disciplina: Sistemas Operacionais
 *  Linguagem : C (POSIX - pthreads + semáforos)
 *
 *  ARQUITETURA:
 *  [Ger.0]─┐
 *  [Ger.1]─┼─► [fila_entrada] ─► [Roteadora] ─► [fila_saida[0]] ─► [Proc.0]
 *  [Ger.2]─┘     (buffer 8)                    ├► [fila_saida[1]] ─► [Proc.1]
 *                                               └► [fila_saida[2]] ─► [Proc.2]
 *                         [Monitor] (estatísticas a cada 2s)
 *
 *  SINCRONIZAÇÃO:
 *   mutex_entrada        → protege fila_entrada (RC entre geradoras)
 *   mutex_saida[i]       → protege cada fila_saida[i] (RC roteadora/proc)
 *   mutex_stats          → protege contadores globais
 *   mutex_id             → protege id_pacote_global (RC entre geradoras)
 *   sem_cheio_entrada    → vagas livres no buffer de entrada  (init=8)
 *   sem_vazio_entrada    → pacotes disponíveis na entrada     (init=0)
 *   sem_cheio_saida[i]   → vagas livres no buffer de saída i  (init=6)
 *   sem_vazio_saida[i]   → pacotes disponíveis na saída i     (init=0)
 *
 *  Compilação: gcc -Wall -Wextra -o roteador roteador.c -lpthread
 *  Execução  : ./roteador
 * ================================================================
 */

#define _POSIX_C_SOURCE 200809L

#include <errno.h>
#include <stdbool.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <semaphore.h>
#include <time.h>

/* ── Verificação de erros POSIX ─────────────────────────────── */
#define PTHREAD_CHECK(cmd, msg)                                      \
    do {                                                             \
        int codigo_erro = (cmd);                                     \
        if (codigo_erro != 0) {                                      \
            fprintf(stderr, "%s: %s\n", (msg), strerror(codigo_erro)); \
            exit(EXIT_FAILURE);                                      \
        }                                                            \
    } while (0)

#define SYSTEM_CHECK(cmd, msg)           \
    do {                                  \
        if ((cmd) == -1) {                \
            perror(msg);                  \
            exit(EXIT_FAILURE);           \
        }                                 \
    } while (0)

/* ── Configuração ────────────────────────────────────────────── */
#define N_GERADORES       3
#define N_DESTINOS        3
#define TAM_FILA_ENTRADA  8
#define TAM_FILA_SAIDA    6
#define MAX_PACOTES      30
#define INTERVALO_MONITOR 2

/* ── Estrutura de pacote ─────────────────────────────────────── */
typedef struct {
    int  id;
    int  origem;
    int  destino;
    int  tamanho;
    char dados[32];
} Pacote;

/* ── Fila circular ───────────────────────────────────────────── */
#define MAX_CAP (TAM_FILA_ENTRADA > TAM_FILA_SAIDA \
                 ? TAM_FILA_ENTRADA : TAM_FILA_SAIDA)
typedef struct {
    Pacote itens[MAX_CAP];
    int    cabeca;
    int    cauda;
    int    count;
    int    capacidade;
} Fila;

/* ── Recursos compartilhados ─────────────────────────────────── */
static Fila            fila_entrada;
static pthread_mutex_t mutex_entrada      = PTHREAD_MUTEX_INITIALIZER;
static sem_t           sem_cheio_entrada;
static sem_t           sem_vazio_entrada;

static Fila            fila_saida[N_DESTINOS];
static pthread_mutex_t mutex_saida[N_DESTINOS];
static sem_t           sem_cheio_saida[N_DESTINOS];
static sem_t           sem_vazio_saida[N_DESTINOS];

static int             total_gerados     = 0;
static int             total_roteados    = 0;
static int             total_processados = 0;
static int             pacotes_por_dest[N_DESTINOS];
static pthread_mutex_t mutex_stats       = PTHREAD_MUTEX_INITIALIZER;

/* Estado atômico de encerramento, compartilhado entre roteador e monitor. */
static atomic_bool     sistema_ativo     = ATOMIC_VAR_INIT(true);
static int             id_pacote_global  = 0;
static pthread_mutex_t mutex_id          = PTHREAD_MUTEX_INITIALIZER;

/* ── Auxiliares de tempo, aleatoriedade e semáforos ─────────── */
static unsigned int prng_proximo(unsigned int *estado) {
    *estado = (*estado * 1103515245u) + 12345u;
    return *estado;
}

static int aleatorio_entre(unsigned int *estado, int minimo, int maximo) {
    unsigned int amplitude = (unsigned int)(maximo - minimo + 1);
    return minimo + (int)((prng_proximo(estado) / 65536u) % amplitude);
}

static void aguardar_ms(unsigned int milissegundos) {
    struct timespec restante = {
        .tv_sec = (time_t)(milissegundos / 1000u),
        .tv_nsec = (long)(milissegundos % 1000u) * 1000000L
    };

    while (nanosleep(&restante, &restante) == -1 && errno == EINTR) {
        /* Retoma apenas o tempo restante se o sono for interrompido. */
    }
}

static void aguardar_semaforo(sem_t *semaforo, const char *mensagem) {
    int resultado;
    do {
        resultado = sem_wait(semaforo);
    } while (resultado == -1 && errno == EINTR);

    if (resultado == -1) {
        perror(mensagem);
        exit(EXIT_FAILURE);
    }
}

/* ── Auxiliares de fila ───────────────────────────────────────── */
static void fila_init(Fila *f, int cap) {
    f->cabeca = f->cauda = f->count = 0;
    f->capacidade = cap;
}

static void fila_enqueue(Fila *f, Pacote p) {
    f->itens[f->cauda] = p;
    f->cauda = (f->cauda + 1) % f->capacidade;
    f->count++;
}

static Pacote fila_dequeue(Fila *f) {
    Pacote p = f->itens[f->cabeca];
    f->cabeca = (f->cabeca + 1) % f->capacidade;
    f->count--;
    return p;
}

/* ================================================================
 *  THREAD GERADORA  (Produtora)
 *  Gera pacotes com IDs únicos e destinos aleatórios, inserindo-os
 *  na fila_entrada via padrão produtor-consumidor.
 * ================================================================ */
static void *thread_geradora(void *arg) {
    int meu_id = *(int *)arg;
    int cota   = MAX_PACOTES / N_GERADORES;
    unsigned int estado = (unsigned int)time(NULL) ^
                          (unsigned int)(meu_id + 1) * 2654435761u;
    if (meu_id == N_GERADORES - 1)
        cota += MAX_PACOTES % N_GERADORES;

    for (int i = 0;
         i < cota && atomic_load_explicit(&sistema_ativo, memory_order_acquire);
         i++) {
        aguardar_ms((unsigned int)aleatorio_entre(&estado, 50, 149));

        /* ── RC: id_pacote_global (mutex_id) ───────────────────
         * Sem este mutex, duas geradoras executando id++
         * simultaneamente leriam o mesmo valor e gerariam IDs
         * duplicados (lost update). */
        PTHREAD_CHECK(pthread_mutex_lock(&mutex_id), "mutex_id lock");
        int pid = id_pacote_global++;
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_id), "mutex_id unlock");

        Pacote p;
        p.id      = pid;
        p.origem  = meu_id;
        p.destino = aleatorio_entre(&estado, 0, N_DESTINOS - 1);
        p.tamanho = aleatorio_entre(&estado, 64, 1087);
        snprintf(p.dados, sizeof(p.dados), "PKT-%03d-G%d", p.id, meu_id);

        /* ── RC: fila_entrada (sem_cheio + mutex_entrada) ───────
         * sem_wait bloqueia sem busy-wait se o buffer estiver
         * cheio. mutex_entrada impede que duas geradoras atualizem
         * cabeca/cauda/count simultaneamente. */
        aguardar_semaforo(&sem_cheio_entrada, "sem_wait cheio_entrada");
        PTHREAD_CHECK(pthread_mutex_lock(&mutex_entrada), "mutex_entrada lock");
        fila_enqueue(&fila_entrada, p);
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_entrada), "mutex_entrada unlock");
        SYSTEM_CHECK(sem_post(&sem_vazio_entrada), "sem_post vazio_entrada");

        PTHREAD_CHECK(pthread_mutex_lock(&mutex_stats), "mutex_stats lock");
        total_gerados++;
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_stats), "mutex_stats unlock");

        printf("[GERADOR %d] Pacote #%03d -> destino %d (%d bytes)\n",
               meu_id, p.id, p.destino, p.tamanho);
        fflush(stdout);
    }
    printf("[GERADOR %d] Encerrado.\n", meu_id);
    return NULL;
}

/* ================================================================
 *  THREAD ROTEADORA  (Consumidora/Produtora híbrida)
 *  Retira pacotes da fila_entrada e os encaminha para a
 *  fila_saida correta conforme o campo `destino` do pacote.
 * ================================================================ */
static void *thread_roteadora(void *arg) {
    (void)arg;
    for (int tratados = 0; tratados < MAX_PACOTES; tratados++) {

        /* ── RC: consumo da fila_entrada ───────────────────────*/
        aguardar_semaforo(&sem_vazio_entrada, "sem_wait vazio_entrada");
        PTHREAD_CHECK(pthread_mutex_lock(&mutex_entrada), "mutex_entrada lock");
        Pacote p = fila_dequeue(&fila_entrada);
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_entrada), "mutex_entrada unlock");
        SYSTEM_CHECK(sem_post(&sem_cheio_entrada), "sem_post cheio_entrada");

        int dest = p.destino % N_DESTINOS;
        printf("[ROTEADOR]  Pacote #%03d de G%d -> fila_saida[%d]\n",
               p.id, p.origem, dest);
        fflush(stdout);

        /* ── RC: produção na fila_saida[dest] ──────────────────
         * ORDEM CORRETA: sem_wait ANTES de mutex_lock.
         * Inverter essa ordem (mutex antes de sem_wait) com
         * buffer cheio cria as 4 condições de Coffman e
         * provoca deadlock (ver comentário no final do arquivo). */
        aguardar_semaforo(&sem_cheio_saida[dest], "sem_wait cheio_saida");
        PTHREAD_CHECK(pthread_mutex_lock(&mutex_saida[dest]), "mutex_saida lock");
        fila_enqueue(&fila_saida[dest], p);
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_saida[dest]), "mutex_saida unlock");
        SYSTEM_CHECK(sem_post(&sem_vazio_saida[dest]), "sem_post vazio_saida");

        PTHREAD_CHECK(pthread_mutex_lock(&mutex_stats), "mutex_stats lock");
        total_roteados++;
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_stats), "mutex_stats unlock");
    }

    /* Sinaliza fim para todas as processadoras */
    atomic_store_explicit(&sistema_ativo, false, memory_order_release);
    for (int i = 0; i < N_DESTINOS; i++)
        SYSTEM_CHECK(sem_post(&sem_vazio_saida[i]), "sem_post encerramento");

    printf("[ROTEADOR]  Todos os %d pacotes roteados. Encerrando.\n",
           MAX_PACOTES);
    return NULL;
}

/* ── Args para thread_processadora ──────────────────────────── */
typedef struct { int id; } ArgsProc;

/* ================================================================
 *  THREAD PROCESSADORA  (Consumidora final)
 *  Consome pacotes da sua fila_saida[id] e simula encaminhamento.
 * ================================================================ */
static void *thread_processadora(void *arg) {
    int id = ((ArgsProc *)arg)->id;

    while (1) {
        /* ── RC: consumo da fila_saida[id] ─────────────────────*/
        aguardar_semaforo(&sem_vazio_saida[id], "sem_wait vazio_saida");

        PTHREAD_CHECK(pthread_mutex_lock(&mutex_saida[id]), "mutex_saida lock");
        if (fila_saida[id].count == 0) {
            PTHREAD_CHECK(pthread_mutex_unlock(&mutex_saida[id]), "mutex_saida unlock");
            if (!atomic_load_explicit(&sistema_ativo, memory_order_acquire)) break;
            continue;
        }
        Pacote p = fila_dequeue(&fila_saida[id]);
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_saida[id]), "mutex_saida unlock");
        SYSTEM_CHECK(sem_post(&sem_cheio_saida[id]), "sem_post cheio_saida");

        unsigned int estado = (unsigned int)(p.id + 1) * 2246822519u;
        aguardar_ms((unsigned int)aleatorio_entre(&estado, 30, 109));

        printf("[PROC   %d]  Pacote #%03d processado (%s, %d bytes)\n",
               id, p.id, p.dados, p.tamanho);
        fflush(stdout);

        PTHREAD_CHECK(pthread_mutex_lock(&mutex_stats), "mutex_stats lock");
        total_processados++;
        pacotes_por_dest[id]++;
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_stats), "mutex_stats unlock");
    }
    printf("[PROC   %d]  Encerrado.\n", id);
    return NULL;
}

/* ================================================================
 *  THREAD MONITOR  (Observador periódico)
 * ================================================================ */
static void *thread_monitor(void *arg) {
    (void)arg;
    while (atomic_load_explicit(&sistema_ativo, memory_order_acquire)) {
        aguardar_ms(INTERVALO_MONITOR * 1000u);
        if (!atomic_load_explicit(&sistema_ativo, memory_order_acquire)) {
            break;
        }
        PTHREAD_CHECK(pthread_mutex_lock(&mutex_stats), "mutex_stats lock");
        printf("\n--- MONITOR | Ger:%d  Rot:%d  Proc:%d ---\n",
               total_gerados, total_roteados, total_processados);
        for (int i = 0; i < N_DESTINOS; i++)
            printf("  Destino[%d]: %d pacotes\n", i, pacotes_por_dest[i]);
        fflush(stdout);
        PTHREAD_CHECK(pthread_mutex_unlock(&mutex_stats), "mutex_stats unlock");
    }
    return NULL;
}

/* ================================================================
 *  INICIALIZAÇÃO
 * ================================================================ */
static void inicializar(void) {
    fila_init(&fila_entrada, TAM_FILA_ENTRADA);
    SYSTEM_CHECK(sem_init(&sem_cheio_entrada, 0, TAM_FILA_ENTRADA), "sem_init cheio_ent");
    SYSTEM_CHECK(sem_init(&sem_vazio_entrada, 0, 0), "sem_init vazio_ent");

    for (int i = 0; i < N_DESTINOS; i++) {
        fila_init(&fila_saida[i], TAM_FILA_SAIDA);
        PTHREAD_CHECK(pthread_mutex_init(&mutex_saida[i], NULL), "mutex_saida init");
        SYSTEM_CHECK(sem_init(&sem_cheio_saida[i], 0, TAM_FILA_SAIDA), "sem_init cheio_s");
        SYSTEM_CHECK(sem_init(&sem_vazio_saida[i], 0, 0), "sem_init vazio_s");
        pacotes_por_dest[i] = 0;
    }
}

/* ================================================================
 *  FINALIZAÇÃO
 * ================================================================ */
static void finalizar(void) {
    SYSTEM_CHECK(sem_destroy(&sem_cheio_entrada), "sem_destroy cheio_entrada");
    SYSTEM_CHECK(sem_destroy(&sem_vazio_entrada), "sem_destroy vazio_entrada");
    for (int i = 0; i < N_DESTINOS; i++) {
        PTHREAD_CHECK(pthread_mutex_destroy(&mutex_saida[i]), "mutex_saida destroy");
        SYSTEM_CHECK(sem_destroy(&sem_cheio_saida[i]), "sem_destroy cheio_saida");
        SYSTEM_CHECK(sem_destroy(&sem_vazio_saida[i]), "sem_destroy vazio_saida");
    }
    PTHREAD_CHECK(pthread_mutex_destroy(&mutex_entrada), "mutex_entrada destroy");
    PTHREAD_CHECK(pthread_mutex_destroy(&mutex_stats), "mutex_stats destroy");
    PTHREAD_CHECK(pthread_mutex_destroy(&mutex_id), "mutex_id destroy");
}

/* ================================================================
 *  MAIN
 * ================================================================ */
int main(void) {
    printf("=== Sistema de Roteamento de Pacotes Multithread ===\n");
    printf("Geradores:%d  Destinos:%d  Total de pacotes:%d\n\n",
           N_GERADORES, N_DESTINOS, MAX_PACOTES);

    inicializar();

    pthread_t thr_ger[N_GERADORES];
    pthread_t thr_rot;
    pthread_t thr_proc[N_DESTINOS];
    pthread_t thr_mon;

    int      ids[N_GERADORES];
    ArgsProc args[N_DESTINOS];

    /* Cria roteadora e processadoras ANTES das geradoras */
    PTHREAD_CHECK(pthread_create(&thr_rot, NULL, thread_roteadora, NULL),
                  "pthread_create roteadora");

    for (int i = 0; i < N_DESTINOS; i++) {
        args[i].id = i;
        PTHREAD_CHECK(pthread_create(&thr_proc[i], NULL, thread_processadora, &args[i]),
                      "pthread_create processadora");
    }

    PTHREAD_CHECK(pthread_create(&thr_mon, NULL, thread_monitor, NULL),
                  "pthread_create monitor");

    for (int i = 0; i < N_GERADORES; i++) {
        ids[i] = i;
        PTHREAD_CHECK(pthread_create(&thr_ger[i], NULL, thread_geradora, &ids[i]),
                      "pthread_create geradora");
    }

    /* Join em ordem: geradoras → roteadora → processadoras → monitor */
    for (int i = 0; i < N_GERADORES; i++)
        PTHREAD_CHECK(pthread_join(thr_ger[i], NULL), "pthread_join geradora");
    PTHREAD_CHECK(pthread_join(thr_rot, NULL), "pthread_join roteadora");
    for (int i = 0; i < N_DESTINOS; i++)
        PTHREAD_CHECK(pthread_join(thr_proc[i], NULL), "pthread_join processadora");

    PTHREAD_CHECK(pthread_join(thr_mon, NULL), "pthread_join monitor");

    printf("\n=== RELATORIO FINAL ===\n");
    printf("Gerados    : %d\n", total_gerados);
    printf("Roteados   : %d\n", total_roteados);
    printf("Processados: %d\n", total_processados);
    for (int i = 0; i < N_DESTINOS; i++)
        printf("  Destino[%d]: %d\n", i, pacotes_por_dest[i]);

    finalizar();
    printf("Sistema encerrado com sucesso.\n");
    return 0;
}

/*
 * ================================================================
 *  ESTUDO DO CAOS — COMO PROVOCAR DEADLOCK (não executar em produção)
 *
 *  1. Alterar: #define TAM_FILA_SAIDA  1   (buffer mínimo)
 *
 *  2. Na thread_roteadora, INVERTER a ordem na região crítica de saída:
 *
 *     ORIGINAL (correto):
 *       sem_wait(&sem_cheio_saida[dest]);        // 1. aguarda vaga
 *       pthread_mutex_lock(&mutex_saida[dest]);  // 2. trava mutex
 *
 *     INVERTIDO (deadlock):
 *       pthread_mutex_lock(&mutex_saida[dest]);  // 1. trava mutex
 *       sem_wait(&sem_cheio_saida[dest]);         // 2. aguarda vaga  ← BLOQUEIA
 *
 *  Resultado: roteadora trava o mutex e bloqueia no semáforo;
 *  processadora obtém o semáforo e bloqueia no mutex → ciclo fechado.
 *
 *  Condições de Coffman satisfeitas:
 *   1. Exclusão mútua   : mutex_saida[i] é não-compartilhável
 *   2. Hold & Wait      : roteadora retém mutex e espera semáforo
 *   3. Não preempção    : ninguém libera o mutex pela roteadora
 *   4. Espera circular  : roteadora→mutex→proc→semáforo→roteadora
 * ================================================================
 */

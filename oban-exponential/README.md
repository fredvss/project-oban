# project-oban-exponential

Evolução do [`oban-basic`](../oban-basic/) com três melhorias principais: **backoff exponencial** entre retentativas, **múltiplas filas nomeadas** com concorrências independentes e **relatório de jobs finalizados**.

---

## O que mudou em relação ao básico

| Funcionalidade | Basic | Exponential |
|----------------|-------|-------------|
| Retry | Imediato | Backoff exponencial (`2^attempt` segundos) |
| Múltiplas filas | ✗ | ✓ (nomes arbitrários) |
| Histórico de jobs | ✗ | ✓ (`:success` e `:failed`) |
| Timestamps por job | ✗ | ✓ (`inserted_at`, `started_at`, `finished_at`) |
| Relatório | ✗ | ✓ (`get_report/1`) |

---

## Arquitetura

```
MiniOban.Application (Supervisor)
├── MiniOban.Queue (name: MiniOban.Queue, max_concurrency: 3)   # fila padrão
└── MiniOban.Queue (name: :critical,      max_concurrency: 1)   # fila prioritária

Cada Queue:
├── pending: [%Job{}, ...]
├── running: %{ref => %Job{}}
├── completed: [%Job{status: :success | :failed}]
└── max_concurrency: N
        │
        └── Task.async ──► Worker.perform/1
                │
                └── falha → Process.send_after(self, {:requeue, job}, delay_ms)
```

---

## Backoff exponencial

A fórmula usada é $2^{\text{attempt}} \times 1000\text{ms}$:

| Tentativa | Espera |
|-----------|--------|
| 1ª retry  | 2s     |
| 2ª retry  | 4s     |
| 3ª retry  | 8s     |

```elixir
delay_ms = trunc(:math.pow(2, next_attempt) * 1_000)
Process.send_after(self(), {:requeue, retry_job}, delay_ms)
```

O job fica **fora da fila** durante a espera — não ocupa slot nem posição.

---

## Módulos

### `MiniOban.Job`

```elixir
%MiniOban.Job{
  id: make_ref(),
  type: :send_email,
  payload: %{},
  inserted_at: ~U[...],   # quando foi enfileirado
  started_at:  ~U[...],   # quando começou a executar
  finished_at: ~U[...],   # quando terminou (sucesso ou falha)
  attempts: 0,
  max_attempts: 3,
  status: :pending        # :pending | :running | :success | :failed
}
```

### `MiniOban.Queue`

| Função | Descrição |
|--------|-----------|
| `start_link(opts)` | Aceita `name:` e `max_concurrency:` (padrão: 5) |
| `queue_job(%Job{}, queue)` | Enfileira em uma fila específica (padrão: `MiniOban.Queue`) |
| `get_state(queue)` | Retorna `%{pending, running, max_concurrency}` |
| `get_report(queue)` | Retorna jobs finalizados separados por status |

---

## Como rodar

```bash
cd project-oban-exponential
iex -S mix
```

### Usando múltiplas filas

```elixir
# Fila padrão (3 slots)
MiniOban.Queue.queue_job(%MiniOban.Job{type: :send_email, payload: %{}})

# Fila crítica (1 slot — execução sequencial)
MiniOban.Queue.queue_job(%MiniOban.Job{type: :billing, payload: %{}}, :critical)
```

### Consultando o relatório

```elixir
MiniOban.Queue.get_report()
# %{
#   success: [%MiniOban.Job{status: :success, ...}, ...],
#   failed:  [%MiniOban.Job{status: :failed, ...}, ...],
#   total_success: 7,
#   total_failed: 3
# }
```

### Exemplo de output com backoff

```
[Queue] Queued job #Reference<0.1.2.3> | type=send_email
[Queue] Dispatching job #Reference<0.1.2.3> | running=1/3
[Worker] Starting job #Reference<0.1.2.3> | type=send_email | attempt=1/3 | will take 1200ms
[Worker] Job #Reference<0.1.2.3> failed
[Queue] Job #Reference<0.1.2.3> failed (random failure) — retrying in 2000ms (attempt 1/3)
# ... 2 segundos depois ...
[Queue] Requeueing job #Reference<0.1.2.3> after exponential backoff
[Queue] Dispatching job #Reference<0.1.2.3> | running=1/3
[Worker] Starting job #Reference<0.1.2.3> | type=send_email | attempt=2/3 | will take 890ms
[Worker] Job #Reference<0.1.2.3> succeeded
[Queue] Job #Reference<0.1.2.3> succeeded | attempt=2 | duration=890ms
```

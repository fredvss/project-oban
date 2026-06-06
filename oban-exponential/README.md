# oban-exponential

Evolução do [`oban-basic`](../oban-basic/) com três melhorias principais: **backoff exponencial** entre retentativas, **múltiplas filas nomeadas** com concorrências independentes e **relatório de jobs finalizados**.

Recomendado ler o README do `oban-basic` primeiro para entender as bases.

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

## O que mudou — passo a passo

### 1. Backoff exponencial com `Process.send_after/3`

No `oban-basic`, o retry é imediato (`send(self(), {:requeue, job})`). Aqui o job fica **suspenso** durante a espera:

```elixir
delay_ms = trunc(:math.pow(2, next_attempt) * 1_000)
Process.send_after(self(), {:requeue, retry_job}, delay_ms)
# ^ agenda a mensagem para daqui a delay_ms milissegundos
# o job NÃO está em pending nem em running durante esse tempo
```

Quando o timer dispara, `{:requeue, job}` chega na caixa de mensagens e o `handle_info({:requeue, job})` o recoloca em `pending`. Isso evita marteleamento no serviço que está falhando.

| Tentativa | Fórmula | Espera |
|-----------|---------|--------|
| 1ª retry  | $2^1$   | 2s     |
| 2ª retry  | $2^2$   | 4s     |
| 3ª retry  | $2^3$   | 8s     |

### 2. Múltiplas filas com o mesmo GenServer

O mesmo módulo `MiniOban.Queue` pode ser iniciado várias vezes com nomes diferentes:

```elixir
# application.ex
children = [
  Supervisor.child_spec({MiniOban.Queue, [name: MiniOban.Queue, max_concurrency: 3]}, id: :queue_default),
  Supervisor.child_spec({MiniOban.Queue, [name: :critical,      max_concurrency: 1]}, id: :queue_critical)
]
```

O `Supervisor.child_spec/2` com `id:` diferente é necessário porque o Supervisor usa o ID para identificar filhos — dois filhos com o mesmo módulo teriam o mesmo ID por padrão, gerando erro.

### 3. `completed` e `get_report/1`

O estado agora inclui `completed: []` onde jobs finalizados são armazenados em memória:

```elixir
MiniOban.Queue.get_report()
# %{
#   success: [%MiniOban.Job{status: :success, ...}],
#   failed:  [%MiniOban.Job{status: :failed,  ...}],
#   total_success: 7,
#   total_failed: 3
# }
```

---

## Cores no output

| Cor | Evento |
|-----|--------|
| 🔵 ciano | job enfileirado |
| 🟡 amarelo | despachando / requeueing após backoff |
| 🟢 verde | job ou worker teve sucesso |
| 🟣 magenta | falha com retry agendado |
| 🔴 vermelho | falha permanente / crash definitivo |

---

## Módulos

### `MiniOban.Job`

```elixir
%MiniOban.Job{
  id: 42,
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
cd oban-exponential
iex -S mix
```

### Usando múltiplas filas

```elixir
# Fila padrão (3 slots)
MiniOban.Queue.queue_job(%MiniOban.Job{type: :send_email, payload: %{}})

# Fila crítica (1 slot — execução sequencial)
MiniOban.Queue.queue_job(%MiniOban.Job{type: :billing, payload: %{}}, :critical)
```

### Exemplo de output

```
[Queue] Queued job 42 | type=send_email
[Queue] Dispatching job 42 | running=1/3
[Worker] Starting job 42 | type=send_email | attempt=1/3 | will take 1200ms
[Worker] Job 42 failed
[Queue] Job 42 failed (random failure) — retrying in 2000ms (attempt 1/3)
# ... 2 segundos depois ...
[Queue] Requeueing job 42 after exponential backoff
[Queue] Dispatching job 42 | running=1/3
[Worker] Starting job 42 | type=send_email | attempt=2/3 | will take 890ms
[Worker] Job 42 succeeded
[Queue] Job 42 succeeded | attempt=2 | duration=890ms
```


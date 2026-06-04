# MiniOban

Um sistema de fila de jobs assíncrono construído em Elixir, inspirado no [Oban](https://github.com/oban-bg/oban). Projeto de estudo para entender como funcionam filas de jobs, concorrência controlada e retry automático usando primitivas do OTP (`GenServer`, `Task`, `Supervisor`).

---

## Arquitetura

```
MiniOban.Application (Supervisor)
└── MiniOban.Queue (GenServer)
        │
        ├── pending: [%Job{}, ...]       # fila FIFO de jobs aguardando
        ├── running: %{ref => %Job{}}    # tasks em execução no momento
        └── max_concurrency: N           # limite de execuções paralelas
                │
                └── Task.async ──► MiniOban.Worker.perform/1
```

O fluxo principal é:

1. Um job é enfileirado via `MiniOban.Queue.queue_job/1`
2. A Queue dispara `:dispatch` internamente
3. O dispatcher calcula quantas vagas há (`max_concurrency - running`) e lança uma `Task` para cada job
4. Quando a Task termina, a Queue recebe o resultado e decide: marcar como sucesso, retentar ou marcar como falha permanente

---

## Módulos

### `MiniOban.Job`

Struct que representa um job na fila.

```elixir
%MiniOban.Job{
  id: make_ref(),          # identificador único (gerado automaticamente se nil)
  type: :send_email,       # átomo que identifica o tipo do job
  payload: %{},            # dados arbitrários para o worker
  attempts: 0,             # número de tentativas já realizadas
  max_attempts: 3,         # máximo de tentativas antes de falha permanente
  status: :pending         # :pending | :running | :success | :failed
}
```

### `MiniOban.Queue`

GenServer responsável por todo o controle da fila.

| Função | Descrição |
|--------|-----------|
| `start_link(opts)` | Inicia a fila. Aceita `max_concurrency: N` (padrão: 5) |
| `queue_job(%Job{})` | Enfileira um job (async, não bloqueia) |
| `get_state()` | Retorna o estado atual: `%{pending, running, max_concurrency}` |

**Comportamentos internos:**

- **`:dispatch`** — Verifica vagas disponíveis e lança Tasks para os próximos jobs pendentes
- **`{ref, result}`** — Recebe o resultado de uma Task concluída; lida com sucesso ou retry
- **`{:DOWN, ref, ...}`** — Captura crash inesperado de uma Task e aplica a mesma lógica de retry

### `MiniOban.Worker`

Simula a execução de um job: aguarda um tempo aleatório (até 2 segundos) e retorna `:ok` ou `{:error, "random failure"}` com 50% de chance cada.

```elixir
MiniOban.Worker.perform(%MiniOban.Job{})
# => :ok | {:error, "random failure"}
```

---

## Como rodar

```bash
mix deps.get
iex -S mix
```

### Enfileirando jobs

```elixir
# Enfileira 10 jobs de uma vez
for i <- 1..10 do
  MiniOban.Queue.queue_job(%MiniOban.Job{
    type: :send_email,
    payload: %{to: "user#{i}@example.com"}
  })
end
```

### Inspecionando o estado

```elixir
MiniOban.Queue.get_state()
# %{
#   pending: [...],
#   running: %{#Reference<...> => %MiniOban.Job{...}},
#   max_concurrency: 3
# }
```

### Exemplo de output

```
[Queue] Queued job #Reference<0.1.2.3> | type=send_email
[Queue] Dispatching job #Reference<0.1.2.3> | running=1/3
[Worker] Starting job #Reference<0.1.2.3> | type=send_email | attempt=1/3 | will take 1423ms
[Worker] Job #Reference<0.1.2.3> failed
[Queue] Job #Reference<0.1.2.3> failed (random failure) — retrying (1/3)
[Queue] Dispatching job #Reference<0.1.2.3> | running=1/3
[Worker] Starting job #Reference<0.1.2.3> | type=send_email | attempt=2/3 | will take 876ms
[Worker] Job #Reference<0.1.2.3> succeeded
[Queue] Job #Reference<0.1.2.3> succeeded after 2 attempt(s)
```

---

## Lógica de Retry

Quando um job falha (seja por retorno `{:error, reason}` ou crash da Task):

- Se `attempts + 1 < max_attempts` → o job é recolocado no final da fila com `attempts` incrementado
- Se `attempts + 1 >= max_attempts` → o job é descartado como falha permanente

O retry acontece imediatamente na próxima rodada de `:dispatch`.

---

## Próximos passos possíveis

- [ ] Backoff exponencial no retry (`attempts * 1000ms` de espera)
- [ ] Histórico de jobs finalizados (`:success` e `:failed`) em memória
- [ ] Múltiplas filas nomeadas com concorrências independentes
- [ ] Telemetria com `:telemetry` para métricas de throughput e latência
- [ ] Persistência de jobs com ETS para sobreviver a restarts do GenServer

---

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `mini_oban` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:mini_oban, "~> 0.1.0"}
  ]
end
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/mini_oban>.


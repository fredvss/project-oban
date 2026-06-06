# project-oban-basic

Implementação inicial de uma fila de jobs assíncrona em Elixir, inspirada no [Oban](https://github.com/oban-bg/oban). Foco em entender o modelo de atores do OTP com `GenServer`, `Task` e `Supervisor`.

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

Fluxo:

1. `queue_job/1` enfileira o job e dispara `:dispatch`
2. O dispatcher calcula as vagas (`max_concurrency - running`) e lança uma `Task` por job
3. Quando a Task termina, a Queue recebe o resultado via `handle_info({ref, result})` e decide: sucesso, retry ou falha permanente
4. Crashes inesperados são capturados por `handle_info({:DOWN, ref, ...})` com a mesma lógica

---

## Módulos

### `MiniOban.Job`

```elixir
%MiniOban.Job{
  id: make_ref(),       # gerado automaticamente se nil
  type: :send_email,    # átomo identificando o tipo do job
  payload: %{},         # dados arbitrários para o worker
  attempts: 0,          # tentativas já realizadas
  max_attempts: 3,      # máximo antes de falha permanente
  status: :pending      # :pending | :running
}
```

### `MiniOban.Queue`

| Função | Descrição |
|--------|-----------|
| `start_link(opts)` | Aceita `max_concurrency: N` (padrão: 3) |
| `queue_job(%Job{})` | Enfileira um job (cast, não bloqueia) |
| `get_state()` | Retorna `%{pending, running, max_concurrency}` |

### `MiniOban.Worker`

Simula execução com duração aleatória (até 2s) e 50% de chance de falha:

```elixir
Worker.perform(job) # => :ok | {:error, "random failure"}
```

---

## Como rodar

```bash
cd project-oban-basic
iex -S mix
```

```elixir
for i <- 1..10 do
  MiniOban.Queue.queue_job(%MiniOban.Job{
    type: :send_email,
    payload: %{to: "user#{i}@example.com"}
  })
end
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

## Lógica de retry

Quando um job falha (retorno `{:error, reason}` ou crash da Task):

- `attempts + 1 < max_attempts` → recolocado no final da fila imediatamente com `attempts` incrementado
- `attempts + 1 >= max_attempts` → descartado como falha permanente

> O retry aqui é **imediato** — sem espera entre tentativas. Veja [`oban-exponential`](../oban-exponential/) para a versão com backoff exponencial.

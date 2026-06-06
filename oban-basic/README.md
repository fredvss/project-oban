# oban-basic

Implementação de uma fila de jobs assíncrona usando apenas primitivas OTP — `GenServer`, `Task` e `Supervisor`. Sem dependências externas além de [`bunt`](https://github.com/rrrene/bunt) para saída colorida.

Inspirada no [Oban](https://github.com/oban-bg/oban), mas focada em ser legível e didática.

---

## Arquitetura

```
MiniOban.Application (Supervisor, :one_for_one)
└── MiniOban.Queue (GenServer)
        │
        ├── state.pending         [job1, job2, ...]      # fila FIFO
        ├── state.running         %{ref => job, ...}     # tasks em voo
        ├── state.completed       [job, ...]             # histórico
        └── state.max_concurrency  3                      # slots paralelos
                │
                └── Task.async ──► MiniOban.Worker.perform/1
```

---

## Como funciona — passo a passo

### Estado interno

O GenServer mantém um mapa com quatro campos:

```elixir
%{
  pending:         [],   # jobs aguardando — lista FIFO
  running:         %{},  # map de %{ref => job} em execução agora
  completed:       [],   # jobs finalizados (sucesso ou falha permanente)
  max_concurrency: 3     # máximo de jobs rodando ao mesmo tempo
}
```

`running` é um mapa e não uma lista porque cada `Task.async` gera uma referência única (`ref`) que o BEAM usa para entregar o resultado de volta à Queue.

---

### 1. `queue_job/1` — enfileiramento (roda no processo chamador)

```elixir
def queue_job(%Job{} = job, queue \\ __MODULE__) do
  job = %{job |
    id: job.id || System.unique_integer([:positive]),
    inserted_at: job.inserted_at || DateTime.utc_now()
  }
  GenServer.cast(queue, {:queue_job, job})
end
```

Esta função roda **no processo de quem chamou**, não no GenServer. Garante que o job tem `id` e `inserted_at`, depois envia uma mensagem assíncrona (`cast`) para a Queue. O chamador não bloqueia — segue em frente imediatamente.

---

### 2. `handle_cast({:queue_job, job})` — recebendo o job

```elixir
def handle_cast({:queue_job, job}, state) do
  new_state = %{state | pending: state.pending ++ [job]}
  send(self(), :dispatch)
  {:noreply, new_state}
end
```

A Queue adiciona o job ao fim de `pending` e **manda uma mensagem para si mesma** (`:dispatch`). O `send(self(), ...)` coloca `:dispatch` na caixa de mensagens da própria Queue, que será processada na próxima iteração do loop de mensagens.

---

### 3. `handle_info(:dispatch)` — o despachante

```elixir
slots = state.max_concurrency - map_size(state.running)
{to_run, remaining} = Enum.split(state.pending, slots)

Enum.reduce(to_run, state.running, fn (job, running) ->
  started_job = %{job | status: :running, started_at: DateTime.utc_now()}
  task = Task.async(fn -> Worker.perform(started_job) end)
  Map.put(running, task.ref, started_job)
end)
```

Calcula vagas disponíveis. Para cada vaga:

- Pega o próximo job de `pending`
- Marca como `:running` com `started_at`
- Lança `Task.async` — cria um processo filho **linkado e monitorado** pela Queue
- Salva `task.ref → job` em `running`

`Task.async` é fundamental: a task é linkada ao GenServer (se a Queue morrer, a task morre também) **e** monitorada (o resultado da task, ou seu crash, chega como mensagem).

#### Backpressure na execução

O dispatch só inicia novas tasks enquanto `map_size(running) < max_concurrency`. Jobs que excedem esse limite permanecem em `pending` até um slot liberar — isso limita a pressão sobre workers e recursos externos (CPU, conexões, APIs).

Isso é **backpressure parcial**: a desaceleração acontece na **execução**, não no enfileiramento. `queue_job/1` é um `cast` que sempre aceita jobs; `pending` não tem tamanho máximo e pode crescer em memória se o produtor enfileirar mais rápido do que a fila processa.

---

### 4. `handle_info({ref, result})` — task terminou normalmente

Quando `Worker.perform/1` retorna, o BEAM entrega `{ref, result}` para a Queue.

```
:ok              → move job para completed com :success
{:error, reason} → ainda tem tentativas? agenda retry com delay linear
                   sem tentativas? move para completed com :failed
```

O `Process.demonitor(ref, [:flush])` é chamado primeiro para parar o monitoramento e descartar qualquer mensagem `:DOWN` residual — sem isso, a Queue poderia receber um `:DOWN` fantasma depois.

Ao final, `send(self(), :dispatch)` reabre a vaga para o próximo job pendente.

---

### 5. `handle_info({:DOWN, ref, ...})` — task crashou

Se o processo filho lançou uma excessão ou saiu de forma inesperada (sem retornar), o BEAM envia `:DOWN` em vez de `{ref, result}`. A lógica é idêntica à de falha controlada — o sistema trata crash da mesma forma que `{:error, reason}`.

---

### Por que `send(self(), :dispatch)` em vez de chamar direto?

O GenServer processa **uma mensagem por vez**. Ao usar `send(self(), :dispatch)`, o dispatch entra no final da caixa de mensagens e é processado depois que a mensagem atual terminar. Isso mantém o loop responsívo e evita chamadas recursivas dentro do `handle_*`.

---

### Fluxo visual

```
chamador            Queue (GenServer)          Task (processo filho)
   │                      │                          │
   │─── queue_job ──►ccast─│                          │
   │                      │  pending ++ [job]         │
   │                      │  send(self, :dispatch)    │
   │                      │                          │
   │               :dispatch                          │
   │                      │─── Task.async ─────────►│
   │                      │    running[ref] = job     │── Worker.perform/1
   │                      │                          │
   │                      │◄── {ref, :ok} ───────────│  (sucesso)
   │                      │◄── {ref, {:error,..}} ────│  (erro controlado)
   │                      │◄── {:DOWN, ref, ..} ──────│  (crash)
   │                      │                          │
   │                      │  completed ++ [job]       │
   │                      │  send(self, :dispatch)    │
```

---

## Cores no output

| Cor | Evento |
|-----|--------|
| 🔵 ciano | job enfileirado |
| 🟡 amarelo | despachando / requeueing |
| 🟢 verde | job ou worker teve sucesso |
| 🟣 magenta | falha com retry agendado |
| 🔴 vermelho | falha permanente / crash definitivo |

---

## Módulos

| Módulo | Responsabilidade |
|--------|------------------|
| `MiniOban.Application` | Sobe o Supervisor com a Queue |
| `MiniOban.Job` | Struct com os dados de um job |
| `MiniOban.Queue` | GenServer que controla a fila |
| `MiniOban.Worker` | Executa o job (simula com sleep + falha aleatória) |

### `MiniOban.Job`

```elixir
%MiniOban.Job{
  id: 42,              # inteiro único gerado automaticamente
  type: :send_email,   # átomo identificando o tipo
  payload: %{},        # dados arbitrários
  attempts: 0,         # tentativas já realizadas
  max_attempts: 3,     # máximo antes de falha permanente
  status: :pending     # :pending | :running | :success | :failed
}
```

---

## Como rodar

```bash
cd oban-basic
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

### Inspecionando o estado

```elixir
MiniOban.Queue.get_state()
# %{pending: [...], running: %{...}, max_concurrency: 3}

MiniOban.Queue.get_report()
# %{success: [...], failed: [...], total_success: 7, total_failed: 3}
```

---

## Retry

Quando um job falha (retorno `{:error, reason}` ou crash):

- `attempts + 1 < max_attempts` → recolocado na fila com delay linear: `attempts × 1000ms` (1s, 2s, 3s…)
- `attempts + 1 ≥ max_attempts` → movido para `completed` como `:failed`

O delay entre tentativas é uma forma de **backpressure temporal**: enquanto o timer não dispara, o job não ocupa slot em `running` nem posição em `pending`, reduzindo carga sobre um serviço que está falhando.

> Para backoff exponencial ($2^{\text{attempt}}$ segundos), veja [`oban-exponential`](../oban-exponential/).

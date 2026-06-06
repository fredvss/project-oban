# MiniOban — Estudo de filas de jobs em Elixir

Repositório de estudo com duas implementações progressivas de uma fila de jobs assíncrona inspirada no [Oban](https://github.com/oban-bg/oban), usando apenas primitivas OTP — `GenServer`, `Task` e `Supervisor`. Sem dependências externas.

---

## Projetos

| Pasta | Descrição |
|-------|-----------|
| [`project-oban-basic/`](./project-oban-basic/) | Fila com concorrência controlada e retry imediato |
| [`project-oban-exponential/`](./project-oban-exponential/) | Evolução com backoff exponencial, múltiplas filas nomeadas e relatório de jobs |

---

## Conceitos explorados

- `GenServer` como estado de fila mutable e controlado
- `Task.async` para execução concorrente com limite de slots
- Monitoramento de processos com `Process.demonitor` e `handle_info({:DOWN, ...})`
- Retry automático com recolocação na fila
- Backoff exponencial com `Process.send_after/3`
- Supervisão com `Supervisor` e estratégia `:one_for_one`
- Múltiplas instâncias do mesmo GenServer com nomes diferentes

---

Ver o README de cada projeto para detalhes de uso e arquitetura.


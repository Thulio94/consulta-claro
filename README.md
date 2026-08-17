# Consulta Claro V2

Painel local independente para consultas reais à API Claro, com processamento
persistente, proteção adaptativa e acompanhamento visual dos robôs.

## Como iniciar

1. Encerre qualquer processamento massivo da V1 para não duplicar consultas.
2. Execute `iniciar.bat`.
3. Na primeira inicialização, aguarde a cópia consistente e a estruturação do
   histórico. O banco original não é alterado.
4. O painel abrirá em `http://127.0.0.1:8520`.

O inicializador cria uma `.venv` própria, verifica o certificado HTTPS, importa
as chaves existentes para `.env` e cria `data/consulta_claro_v2.db` usando o
backup online do SQLite.

## Segurança e funcionamento

- O servidor escuta somente em `127.0.0.1`.
- Não há modo mock nem resultados fictícios.
- Toda consulta manual ou massiva exige confirmação explícita e usa a API real.
- As chaves ficam em `.env`, são mascaradas no painel e não são gravadas em logs.
- HTTP 429 ativa resfriamento global e redução automática de velocidade.
- O usuário define diretamente o teto global de requisições por segundo. A
  quantidade de threads controla apenas quantos robôs podem trabalhar ao mesmo
  tempo.
- O usuário também define a espera mínima entre duas consultas do mesmo robô.
  O ritmo final respeita tanto esse espaçamento quanto o teto global em req/s.
- Com a proteção adaptativa ativa, cada 429 reduz a velocidade pela metade,
  respeita `Retry-After` (ou 60 segundos quando ele não vier) e recupera a taxa
  gradualmente sem ultrapassar o teto escolhido.
- Se todas as chaves retornarem 401/403, o trabalho pausa. Corrija o `.env` e
  use **Retomar** para recarregar as chaves sem perder a fila.
- O banco usa WAL, índices, paginação e um escritor em lote.

## Pastas

- `data`: banco independente da V2.
- `exports`: arquivos CSV gerados em segundo plano.
- `app`: API, banco e motor assíncrono.
- `static`: painel web de página única.
- `tests`: testes locais que não chamam a API real.

# FactorySI.SimpleTcp

Biblioteca .NET para comunicação TCP entre clientes e servidores, com suporte a mensagens de texto e binárias.

| Propriedade | Valor |
| --- | --- |
| Package ID | `FactorySI.SimpleTcp` |
| Assembly e namespace | `FactorySI.SimpleTcp` |
| Versão | `2.0.0` |
| Framework de destino | `net462` |
| Licença | Apache-2.0 |

## Origem, licença e atribuição

`FactorySI.SimpleTcp` é um trabalho derivado de [SimpleTCP](https://github.com/BrandonPotter/SimpleTCP), de Brandon Potter. A distribuição foi renomeada, atualizada e empacotada pela FactorySI em 2026.

- Autores do pacote: `BrandonPotter;FactorySI`.
- O texto integral da licença Apache-2.0 está em [LICENSE](LICENSE).
- As atribuições e o aviso de trabalho derivado estão em [NOTICE](NOTICE).

## Instalação por feed local

O pacote oficial desta entrega está disponível pelo feed local `D:\FactorySIPackageNuget`. Esta documentação não declara publicação no nuget.org.

Configure `D:\FactorySIPackageNuget` como uma origem de pacote local no NuGet e instale a versão desejada. Exemplo pela CLI:

```powershell
dotnet add SeuProjeto.csproj package FactorySI.SimpleTcp --version 2.0.0 --source D:\FactorySIPackageNuget
```

Também é possível adicionar essa pasta em **Ferramentas > Gerenciador de Pacotes NuGet > Origens de Pacotes** no Visual Studio e instalar `FactorySI.SimpleTcp` pela interface.

## Exemplo mínimo

Use o namespace da distribuição FactorySI:

```csharp
using FactorySI.SimpleTcp;
```

Servidor que inicia a escuta na porta `8910` e responde a mensagens delimitadas:

```csharp
using FactorySI.SimpleTcp;

var servidor = new SimpleTcpServer();
servidor.Delimiter = 0x13;
servidor.DelimiterDataReceived += (remetente, mensagem) =>
{
    mensagem.ReplyLine("Mensagem recebida: " + mensagem.MessageString);
};

servidor.Start(8910);
```

Cliente que se conecta ao servidor e aguarda uma resposta:

```csharp
using FactorySI.SimpleTcp;

var cliente = new SimpleTcpClient().Connect("127.0.0.1", 8910);
var resposta = cliente.WriteLineAndGetReply("Olá", TimeSpan.FromSeconds(3));
```

## Migração a partir de `SimpleTCP`

Esta versão tem uma mudança de identidade incompatível com a distribuição anterior. Atualize todos os pontos abaixo:

| Identidade anterior | Nova identidade |
| --- | --- |
| Package ID `SimpleTCP` | Package ID `FactorySI.SimpleTcp` |
| Assembly `SimpleTCP` | Assembly `FactorySI.SimpleTcp` |
| `using SimpleTCP;` | `using FactorySI.SimpleTcp;` |

Os nomes públicos `SimpleTcpServer` e `SimpleTcpClient` foram mantidos. Ainda assim, a troca de pacote, assembly e namespace exige recompilação dos projetos consumidores.

## Recebimento e eventos

O recebimento é assíncrono por cliente conectado:

- eventos originados por **clientes diferentes** podem ser executados em paralelo;
- eventos do **mesmo cliente** permanecem sequenciais;
- em protocolos delimitados, concentre o processamento da regra de negócio em `DelimiterDataReceived`;
- não duplique a mesma regra em `DataReceived`, pois isso pode processar uma mensagem delimitada duas vezes.

O delimitador padrão existente é `0x13` (decimal `19`). Ele não corresponde a newline nem ao caractere decimal `13`. Configure `Delimiter` explicitamente quando o protocolo utilizar outro terminador e garanta que cliente e servidor adotem a mesma convenção.

## Configurações relevantes para estabilidade

- `Delimiter`: define o byte que encerra uma mensagem delimitada.
- `MaxDelimiterMessageLength`: limita o tamanho de mensagens delimitadas e deve ser ajustado ao maior tamanho legítimo previsto pelo protocolo.
- `WriteTimeout`: estabelece o limite de tempo para operações de escrita; configure-o de acordo com a latência e a disponibilidade esperadas da conexão.

Essas configurações devem ser definidas de forma compatível entre os participantes do protocolo e validadas em condições de operação representativas, especialmente quando houver mensagens grandes ou redes instáveis.

## Build e testes

Os projetos têm como destino `net462`. É necessário um ambiente de desenvolvimento compatível com esse framework para compilar e testar a solução.

```powershell
# Compilar a solução em Debug
dotnet build .\FactorySI.SimpleTcp.sln --configuration Debug

# Executar os testes
dotnet test .\FactorySI.SimpleTcp.sln --configuration Debug
```

O build em `Release` está configurado para gerar o pacote. A geração falha se já existir na pasta de saída um pacote com o mesmo ID e versão; nesse caso, não sobrescreva o artefato existente sem a decisão de versionamento apropriada.

```powershell
dotnet build .\FactorySI.SimpleTcp.sln --configuration Release
```

## Limitações

Esta biblioteca não substitui um servidor HTTP ou uma plataforma de hospedagem web para uso em produção. Projetos que dependem de protocolos próprios devem definir e testar explicitamente enquadramento de mensagens, delimitador, tamanhos máximos, timeouts e tratamento de desconexões.

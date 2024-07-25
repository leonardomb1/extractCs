
# extractCs

Extrator de dados de SQL Server - de tabelas do Protheus TOTVS para bancos de dados de DataWarehouse.


## Instalação

Requisito .NET 8.0: Instale utilizando:

```pwsh
  git clone https://github.com/leonardomb1/extractCs.git

  dotnet publish --os win --arch x64 --sc true `
  --ucr true -p:PublishSingleFile=true -p:EnableCompressionSingleFile=true `
  -p:IncludeAllContentForSelfExtract=true -p:DebugType=None -p:DebugSymbols=false
  
```

Necessário configurar váriaveis de ambiente:

PACKET_SIZE: Tamanho do pacote enviado do servidor de origem.
DW_CONNECTIONSTRING: Connection String para o bando de dados de staging/extração.
ORQUEST_CONNECTIONSTRING: Connection String para o banco "DWController", padrão para controlar serviço.
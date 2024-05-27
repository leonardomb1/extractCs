
# extractCs

Extrator de dados de SQL Server - de tabelas do Protheus TOTVS para bancos de dados de DataWarehouse.


## Instalação

Requisito .NET 8.0: Instale utilizando, lembre de preencher as connections strings em app.config:

Bash:
```bash
  git clone https://github.com/leonardomb1/extractCs.git

  dotnet publish --os win --arch x64 --sc true `
  --ucr true -p:PublishSingleFile=true -p:EnableCompressionSingleFile=true `
  -p:IncludeAllContentForSelfExtract=true -p:DebugType=None -p:DebugSymbols=false
  
```

PowerShell
```pwsh
  git clone https://github.com/leonardomb1/extractCs.git

  dotnet publish --os win --arch x64 --sc true `
  --ucr true -p:PublishSingleFile=true -p:EnableCompressionSingleFile=true `
  -p:IncludeAllContentForSelfExtract=true -p:DebugType=None -p:DebugSymbols=false
  
```
Necessário colocar variável de ambiente dentro do container.
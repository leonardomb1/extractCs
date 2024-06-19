FROM debian:slim AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:8.0 AS build
WORKDIR /src

COPY ["integraCs.csproj", "./"]
RUN dotnet restore "integraCs.csproj"

COPY . .

RUN dotnet publish "integraCs.csproj" --os linux --arch x64 --sc true --ucr true -o /app/publish -p:PublishSingleFile=true -p:EnableCompressionSingleFile=true -p:IncludeAllContentForSelfExtract=true -p:DebugType=None -p:DebugSymbols=false

FROM base AS final
WORKDIR /app

COPY --from=build /app/publish .

ENTRYPOINT ["./extractCs"]

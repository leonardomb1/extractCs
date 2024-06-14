FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app
USER root

RUN apt update && apt install -y openssh-server && \
    echo "root:Docker!" | chpasswd && \
    mkdir /var/run/sshd && \
    chmod 0755 /var/run/sshd && \
    sed -i 's/^#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config && \
    sed -i 's/^#PermitEmptyPasswords.*/PermitEmptyPasswords no/' /etc/ssh/sshd_config && \
    echo "AllowUsers root" >> /etc/ssh/sshd_config

EXPOSE 2222

CMD ["/usr/sbin/sshd", "-D"]


FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG configuration=Release
WORKDIR /src
USER app

COPY ["integraCs.csproj", "./"]
RUN dotnet restore "integraCs.csproj"

COPY . .

RUN dotnet build "integraCs.csproj" -c $configuration -o /app/build

FROM build AS publish
ARG configuration=Release
RUN dotnet publish "integraCs.csproj" -c $configuration -o /app/publish /p:UseAppHost=false

FROM base AS final
WORKDIR /app

COPY --from=publish /app/publish .

ENTRYPOINT ["dotnet", "integraCs.dll", "--no-launch-profile", "--no-build", "--no-restore"]
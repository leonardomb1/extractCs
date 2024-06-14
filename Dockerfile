FROM mcr.microsoft.com/dotnet/runtime:8.0 AS base
WORKDIR /app
USER app

# Update packages and install OpenSSH server
RUN apt update
RUN apt install -y openssh-server

# Set the root password
RUN echo "root:Docker!" | chpasswd

# Create the /var/run/sshd directory
RUN mkdir /var/run/sshd
RUN chmod 0755 /var/run/sshd

# Modify the SSH daemon configuration
RUN sed -i 's/^#PermitRootLogin.*/PermitRootLogin yes/' /etc/ssh/sshd_config
RUN sed -i 's/^#PasswordAuthentication.*/PasswordAuthentication yes/' /etc/ssh/sshd_config
RUN sed -i 's/^#PubkeyAuthentication.*/PubkeyAuthentication yes/' /etc/ssh/sshd_config
RUN sed -i 's/^#PermitEmptyPasswords.*/PermitEmptyPasswords no/' /etc/ssh/sshd_config

# Allow the root user to login via SSH
RUN echo "AllowUsers root" >> /etc/ssh/sshd_config


EXPOSE 2222

CMD ["/usr/sbin/sshd", "-D"]

FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:8.0 AS build
ARG configuration=Release
WORKDIR /src

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
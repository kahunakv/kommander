FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build-env

RUN mkdir -p /src
#COPY Nixie /src/Nixie/
COPY Kommander /src/Kommander/
COPY Kommander.Server /src/Kommander.Server/

# build the dotnet program
WORKDIR /

RUN cd /src/Kommander.Server/ && dotnet publish -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS runtime
WORKDIR /app

# expose the ports
EXPOSE 8004
EXPOSE 8005

# copy the built program
COPY --from=build-env /app .
COPY certs/development-certificate.pfx /app/certificate.pfx

# install sqlite to debug
RUN apt update && apt upgrade && apt-get -y install sqlite3

ARG KOMMANDER_RAFT_NODEID
ARG KOMMANDER_RAFT_HOST
ARG KOMMANDER_RAFT_PORT
ARG KOMMANDER_HTTP_PORTS
ARG KOMMANDER_HTTPS_PORTS
ARG KOMMANDER_INITIAL_CLUSTER

ENV KOMMANDER_RAFT_NODEID="$KOMMANDER_RAFT_NODEID"
ENV KOMMANDER_RAFT_HOST="$KOMMANDER_RAFT_HOST"
ENV KOMMANDER_RAFT_PORT="$KOMMANDER_RAFT_PORT"
ENV KOMMANDER_HTTP_PORTS="$KOMMANDER_HTTP_PORTS"
ENV KOMMANDER_HTTPS_PORTS="$KOMMANDER_HTTPS_PORTS"
ENV KOMMANDER_INITIAL_CLUSTER="$KOMMANDER_INITIAL_CLUSTER"

COPY --chmod=755 <<EOT /app/entrypoint.sh
#!/usr/bin/env bash
echo "kommander --raft-nodeid $KOMMANDER_RAFT_NODEID --raft-host $KOMMANDER_RAFT_HOST --raft-port $KOMMANDER_RAFT_PORT --http-ports $KOMMANDER_HTTP_PORTS --https-ports $KOMMANDER_HTTPS_PORTS --https-certificate /app/certificate.pfx --initial-cluster $KOMMANDER_INITIAL_CLUSTER --sqlite-wal-path /app/data --sqlite-wal-revision v0"
dotnet /app/Kommander.Server.dll --raft-nodeid $KOMMANDER_RAFT_NODEID --raft-host $KOMMANDER_RAFT_HOST --raft-port $KOMMANDER_RAFT_PORT --http-ports $KOMMANDER_HTTP_PORTS --https-ports $KOMMANDER_HTTPS_PORTS --https-certificate /app/certificate.pfx --initial-cluster $KOMMANDER_INITIAL_CLUSTER --sqlite-wal-path /app/data --sqlite-wal-revision v0
EOT

# when starting the container, run dotnet with the built dll
ENTRYPOINT [ "/app/entrypoint.sh" ]

# Swap entrypoints if the container is exploding and you want to keep it alive indefinitely so you can go look into it.
#ENTRYPOINT ["tail", "-f", "/dev/null"]

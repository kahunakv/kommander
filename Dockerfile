FROM mcr.microsoft.com/dotnet/sdk:9.0-alpine AS build-env

RUN mkdir -p /src
COPY Kommander /src/Kommander/
COPY Kommander.Http /src/Kommander.Http/

# build the dotnet program
WORKDIR /

RUN cd /src/Kommander.Http/ && dotnet publish -c release -o /app

FROM mcr.microsoft.com/dotnet/aspnet:9.0-alpine AS runtime
WORKDIR /app

# expose the health port
EXPOSE 8004 

# copy the built program
COPY --from=build-env /app .

# when starting the container, run dotnet with the built dll
ENTRYPOINT ["dotnet", "/app/Kommander.Http.dll"]

# Swap entrypoints if the container is exploding and you want to keep it alive indefinitely so you can go look into it.
#ENTRYPOINT ["tail", "-f", "/dev/null"]


FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY NuGet.Config Savannah.sln ./
COPY src/Savannah.OrderBook/Savannah.OrderBook.csproj src/Savannah.OrderBook/
RUN dotnet restore src/Savannah.OrderBook/Savannah.OrderBook.csproj
COPY src/Savannah.OrderBook/*.cs src/Savannah.OrderBook/
RUN dotnet publish src/Savannah.OrderBook/Savannah.OrderBook.csproj --no-restore -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "Savannah.OrderBook.dll"]

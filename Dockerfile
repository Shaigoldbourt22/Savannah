FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY NuGet.Config Binance.LocalOrderBook.sln ./
COPY src/Binance.LocalOrderBook/Binance.LocalOrderBook.csproj src/Binance.LocalOrderBook/
RUN dotnet restore src/Binance.LocalOrderBook/Binance.LocalOrderBook.csproj
COPY src/Binance.LocalOrderBook/*.cs src/Binance.LocalOrderBook/
RUN dotnet publish src/Binance.LocalOrderBook/Binance.LocalOrderBook.csproj --no-restore -c Release -o /app

FROM mcr.microsoft.com/dotnet/runtime:10.0
WORKDIR /app
COPY --from=build /app .
USER $APP_UID
ENTRYPOINT ["dotnet", "Binance.LocalOrderBook.dll"]

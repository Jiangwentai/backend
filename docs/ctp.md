# CTP market-data setup

## SDK

Obtain the CTP market-data SDK and permission to use it from your broker or operator. Do not copy proprietary headers or libraries into this repository. Configure a root with this shape:

```text
/opt/ctp-sdk/
  include/ThostFtdcMdApi.h
  lib/libthostmduserapi_se.so
```

The library may instead be under `lib64`, or be named `thostmduserapi`. Build with:

```sh
cmake -S . -B build/ctp -DENABLE_CTP=ON -DCTP_SDK_ROOT=/opt/ctp-sdk
cmake --build build/ctp
```

Official packages that keep the headers and `thostmduserapi_se.so` together in a nested version directory are also detected. For the locally supplied package, either its outer directory or the single version directory can be used:

```sh
cmake -S . -B build/ctp -DENABLE_CTP=ON -DCTP_SDK_ROOT="$PWD/ctp_file"
cmake --build build/ctp -j2
```

`ctp_file/` is excluded from Git and normal Docker build contexts. Keep it local and do not override those exclusions.

Because some official Linux packages ship a library without an ELF SONAME or the standard `lib` prefix, configuration stages a normalized `libthostmduserapi_se.so` beside `market_data_collector`. Keep that generated library beside the executable when moving the build output; do not copy it into source control.

The default is `ENABLE_CTP=OFF`. Enabling it without usable headers and a library is a configure-time error. Authentication support is inferred from the supplied header, not from an assumed SDK version.

## Configuration

Use a local copy of `config/app.yaml` and select `source: ctp`. Configure `ctp.front_address`, `ctp.broker_id`, `ctp.user_id`, `ctp.flow_path`, and subscriptions. A subscription may be written as `SHFE.zn2610` or `zn2610`; only the instrument ID is passed to `SubscribeMarketData`, while `ExchangeID` is preserved from each callback.

Provide secrets through the environment:

```sh
export MARKET_DATA_SOURCE=ctp
export CTP_FRONT_ADDRESS=tcp://operator-host:port
export CTP_BROKER_ID=your-broker
export CTP_USER_ID=your-user
export CTP_PASSWORD='...'
export CTP_AUTHENTICATION_REQUIRED=true
export CTP_APP_ID='...'
export CTP_AUTH_CODE='...'
./build/ctp/market_data_collector /path/to/local-app.yaml
```

Never commit the local configuration, `.env`, password, AppID, or AuthCode. Authentication must be enabled only when required by the selected front and supported by its supplied headers.

## Runtime behavior

The session follows connect, optional authenticate, login, subscribe, and ready states. On disconnect it clears active subscriptions but retains the desired set; the SDK reconnect callback triggers the complete session flow and resubscription.

`TradingDay` and `ActionDay` are preserved independently. A valid `ActionDay + UpdateTime + UpdateMillisec` is interpreted as Asia/Shanghai and stored as UTC. If `ActionDay` is empty, the date nearest to local receive time is selected with a 12-hour crossover boundary. Malformed time data is rejected and counted. CTP price sentinels become internal NaN and QuestDB NULL; cumulative volume and turnover remain cumulative.

The ingress queue remains SPSC: synthetic and CTP inputs are mutually exclusive, and one MdApi instance is assumed to serialize market-data callbacks. Verify that assumption in the documentation supplied with the actual SDK before production deployment.

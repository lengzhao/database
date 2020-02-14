# database

database with flag and can rollback

## Build

1. git clone
2. cd database
3. go build

## command

| Command                |  Description |
|------------------------|:-------------|
| ./database -h          | help         |
| ./database -c stop     | stop service   |
| ./database -c uinstall | uninstall service |
| ./database -c install  | install service   |
| ./database -c start    | start service     |
| ./database -c stat     | show service stat |
| ./database             | only run(not register service) |

## Run

1. change listener port: conf.json
2. stop & uninstall
3. install
4. start
5. check stat

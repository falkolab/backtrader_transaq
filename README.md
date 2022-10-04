# Интеграция Transaq Connector для Backtrader

**Важно!** Модуль находится на стадии разработки и тестирования, но уже функционирует. 
Используйте с осторожностью на свой страх и риск.

Пример:

```python
    from backtrader_transaq.store import TransaqStore
    import os
    from transaqpy.grpc_connector.connector import GRPCTransaqConnector

    # Sending command implemented directly in client
    connector = GRPCTransaqConnector(os.environ['GRPC_SERVER'])
    client_id = os.getenv('STORE_CLIENT', default=None)
    store = TransaqStore(
        transaq_connector=connector,
        login=os.environ['TRANSAQ_LOGIN'],
        password=os.environ['TRANSAQ_PASSWORD'],
        host=os.environ['TRANSAQ_HOST'],
        port=os.environ['TRANSAQ_PORT'],
        reconnect=3,
        reconnect_timeout=10,
        client_id=client_id,
        base_currency='RUB',
        # Если у вас есть расхождение с сервером и у вас нет возможности исправить таймзоны 
        # Во время подключения к серверу в лог будет выведено текущее расхождение
        time_diff_adjustment=-7200,
        notifyall=True
    )
    def change_password_callback(server_status_message):
         result = store.conn.change_pass(old_password="1", new_password="2")
         print(result.__repr__())
    
    store.set_on_connect_callback(change_password_callback)
    store.reconnect()
    # Либо можно вызвать любую другую команду (см. transaqpy.commands для справки)
    # store.conn.send_command(CommandMaker('change_pass', oldpass=old_password, newpass=new_password))
```

Подробные примеры смотрите в backtrader репозитории и документации. 
Например: [ibtest](https://github.com/mementum/backtrader/tree/master/samples/ibtest)

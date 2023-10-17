_______ 17-10-2023 _______

Еще раз привет!

Забегая вперед, я попробовал еще раз прогнать свой старый код и он корректно отработал. Пишет по 8 строк в базу и 8 записей отправляет в топик.
Видимо в прошлый раз из-за большого количества перезапусков при отладке какие-то переменные запомоились. Я не перезапускал ядро. 

Попробовал исправить по твоему шаблону и код упал
Изменил кусок с сохранением kafka_df на:

kafka_df\
        .write\
        .outputMode("append")\
        .format("kafka")\
        .options(**kafka_security_options)\
        .option("topic", topic_out)\
        .trigger(processingTime="15 seconds")\
        .option("checkpointLocation", "/root/spark_checkpoint")\
        .option("truncate", False)\
        .save()

Мне выдало вот такую ошибку:

-------------------------------------------

StreamingQueryException                   Traceback (most recent call last)
/root/project.ipynb Cell 7 line 3
      1 # ___________________ ОТПРАВКА ДАННЫХ В СТОК ___________________
----> 3 result_df.writeStream \
      4     .foreachBatch(foreach_batch_function) \
      5     .start() \
      6     .awaitTermination()

File /usr/local/lib/python3.9/dist-packages/pyspark/sql/streaming.py:107, in StreamingQuery.awaitTermination(self, timeout)
    105     return self._jsq.awaitTermination(int(timeout * 1000))
    106 else:
--> 107     return self._jsq.awaitTermination()

File /usr/local/lib/python3.9/dist-packages/py4j/java_gateway.py:1321, in JavaMember.__call__(self, *args)
   1315 command = proto.CALL_COMMAND_NAME +\
   1316     self.command_header +\
   1317     args_command +\
   1318     proto.END_COMMAND_PART
   1320 answer = self.gateway_client.send_command(command)
-> 1321 return_value = get_return_value(
   1322     answer, self.gateway_client, self.target_id, self.name)
   1324 for temp_arg in temp_args:
   1325     temp_arg._detach()
...
    return_value = get_return_value(
  File "/usr/local/lib/python3.9/dist-packages/pyspark/sql/utils.py", line 196, in deco
    raise converted from None
pyspark.sql.utils.AnalysisException: 'write' can not be called on streaming Dataset/DataFrame

-----------------------------------------------






_______ 16-10-2023 _______

Привет, Светлана.

Я прочитал статью, но ничего там не понял.
В теории нам говорилось дословно следующее:

> Семантика доставки сообщений exactly-once
> Spark Structured Streaming гарантирует, что атомарные датасеты, на которые Spark делит данные при обработке, будут доставлены exactly-once

В итоге получается это не так. Я не знаю как решить эту проблему.



_______ 14-10-2023 _______

Привет ревьювер!

У меня как то странно пишется поток через foreachBatch, не знаю что на это влияет. 
После отправки сообщения во входящий топик, топик-получатель корректно считывает 8 строк данных, а вот в Postgres пишется то 8 строк, то 20 то 30. Не понимаю почему туда с одного переданного сообщения больше данных приходит.
В остальном вроде работает.
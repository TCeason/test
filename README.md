# Dependencies

JDK-17

## Deployment

```sql
create database db;

create table db.t (
id1 int,
id2 int,
id3 int,
id4 int,
id5 int,
id6 int,
id7 int,
id8 int,
id9 int,
id10 int,
id11 int,
id12 int,
id13 int,
id14 int,
id15 int,
id16 int,
str1 String,
str2 String,
str3 String,
dt timestamp
);

```

## usage

the `bench8023` use jdbc-8023

the `bench8028` use jdbc-8028

```
./bench8023 --thread-nums=2 --max-allowed-thread-nums=4 --host=127.0.0.1 --user=root --password=root --port=3307 --batch-rows=1000 --total-rows=60000

OR

./bench8028 --thread-nums=2 --max-allowed-thread-nums=4 --host=127.0.0.1 --user=root --password=root --port=3307 --batch-rows=1000 --total-rows=60000


```
create table city_topn(
day varchar(20) not null,
city varchar(20) not null,
time bigint(10) not null,
primary key(day,city)
)charset utf8;

create table time_topn(
hour varchar(10) not null,
cnt int(10) not null,
primary key(hour,cnt)
)charset=utf8;

create table traffics_topn(
day varchar(20) not null,
url text(300) not null,
traffics bigint(20) not null
)charset=utf8;

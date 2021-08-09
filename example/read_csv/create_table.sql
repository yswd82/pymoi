create table dbo.pymoi_example_read_csv
(
	col_pk int not null,
	col_int_nn int null,
	col_varchar_nn varchar(100) null,
	col_decimal decimal(9,0) null,
	col_datetime datetime null
	primary key
	(
		col_pk
	)
)
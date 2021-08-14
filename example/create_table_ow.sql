CREATE TABLE dbo.pymoi_example_ow(
	[fid] [varchar](4) NOT NULL,
	[fdate] [date] NULL,
	[fcode] [varchar](4) NULL,
	[fprice] [decimal](9, 0) NULL,
	[famount] [decimal](9, 0) NULL,
	[record_id] [int] NOT NULL,
	[is_deleted] [bit] NULL,
PRIMARY KEY CLUSTERED 
(
	[fid] ASC,
	[record_id] ASC
)WITH (PAD_INDEX = OFF, STATISTICS_NORECOMPUTE = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS = ON, ALLOW_PAGE_LOCKS = ON) ON [PRIMARY]
) ON [PRIMARY]

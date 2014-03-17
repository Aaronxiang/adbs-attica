#####################
##Queries with groups
#####################

#for a short query files - create_sm_10_2.sql & create_sm_10_4.sql
select sm_10_4.vInt, sm_10_4.gStr, sm_10_2.vInt, sm_10_2.gStr from sm_10_4, sm_10_2 where sm_10_4.gInt=sm_10_2.gInt; exit;

#for a longer query files - create_sm_10000_10.sql & create_sm_10000_20.sql
select sm_10000_10.vInt, sm_10000_10.gStr, sm_10000_20.vInt, sm_10000_20.gStr from sm_10000_10, sm_10000_20 where sm_10000_10.gInt=sm_10000_20.gInt; exit;

#####################
##Generator data queries
#####################
select a.unique2, a.stringu1, b.unique2, b.stringu2 from a, b where a.unique2=b.unique2

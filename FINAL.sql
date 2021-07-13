--schema bd_radar_comhospi  --criacao do banco de dados

--drop schema if exists bd_radar_comhospi;  --exclui um novo esquema para o banco bd_radar_comhospi
create schema if not exists bd_radar_comhospi;  --cria um novo esquema para o banco bd_radar_comhospi
set search_path to bd_radar_comhospi;  --muda o esquema corrente para o bd_radar_comhospi

-- redes
create table if not exists redes(
  typerede int not null,
  nameRede varchar(45) not null,
  ocupationenf int not null,
  ocupationutiped int not null,
  ocupationutiadul int not null,
  ocupationtotal int not null,
  primary key (typerede));

--locais
create table if not exists locais (
  place_type varchar(5) not null,
  state varchar(2) not null,
  city varchar(45) null,
  city_ibge_code int not null,
  estimated_population int not null,
  primary key (city_ibge_code));

--notificacoes
create table if not exists notificacoes(
  idnotificacao int not null,
  epidemiological_week int not null,
  date date not null,
  total_confirmations int not null,
  last_available_confirmed_per_100k_inhabitants decimal(10,5) not null,
  new_confirmed int not null,
  total_deaths int not null,
  new_deaths int not null,
  last_available_death_rate decimal(5,4) not null,
  city_ibge_code int not null,
  primary key (idnotificacao),
  constraint fk_notificacoes_locais1
    foreign key (city_ibge_code)
    references locais (city_ibge_code)
    on delete cascade
    on update cascade);

--internacoes
create table if not exists internacoes (
  idinternacao int not null,
  observations varchar(200) null,
  idnotificacao int not null,
  typerede int not null,
  ocupation varchar(4) not null,
  primary key (idinternacao),
  constraint fk_internacoes_redes
    foreign key (typerede)
    references redes (typerede)
    on delete cascade
    on update cascade,
  constraint fk_internacoes_notificacoes1
    foreign key (idnotificacao)
    references notificacoes (idnotificacao)
    on delete cascade
    on update cascade);

--todas as tabelas foram criadas caso não existissem e é possivel ver se nelas possuem dados
select * from redes;
select * from locais;
select * from notificacoes;
select * from internacoes;










--funcoes e triggers
-- função para atualizar a qntde de leitos disponíveis em cada rede
--1 funcao atualiza leitos rede publica
create or replace function atualizaocupacoespublica() returns trigger as 
$$
declare
	regatual int;
	ocupenf int;
	ocuputiadul int;
	ocuputiped int;
	totalocupacoes int;
	
begin
	select count(idinternacao) from internacoes i where i.typerede = 0 into totalocupacoes;
	select count(idinternacao) from internacoes i where i.typerede = 0 and i.ocupation = 'enf' into ocupenf;
	select count(idinternacao) from internacoes i where i.typerede = 0 and i.ocupation = 'adul' into ocuputiadul;
	select count(idinternacao) from internacoes i where i.typerede = 0 and i.ocupation = 'ped' into ocuputiped;

	update redes r set ocupationenf = ocupenf where r.typerede = 0;
	update redes r set ocupationutiadul = ocuputiadul where r.typerede = 0;
	update redes r set ocupationutiped = ocuputiped where r.typerede = 0;
	update redes r set ocupationtotal = totalocupacoes where r.typerede = 0;
return new;
end
$$ language plpgsql;

--2 funcao atualiza leitos rede privada
create or replace function atualizaocupacoesprivada() returns trigger as 
$$
declare
	regatual int;
	ocupenf int;
	ocuputiadul int;
	ocuputiped int;
	totalocupacoes int;
	
begin
	select count(idinternacao) from internacoes i where i.typerede = 1 into totalocupacoes;
	select count(idinternacao) from internacoes i where i.typerede = 1 and i.ocupation = 'enf' into ocupenf;
	select count(idinternacao) from internacoes i where i.typerede = 1 and i.ocupation = 'adul' into ocuputiadul;
	select count(idinternacao) from internacoes i where i.typerede = 1 and i.ocupation = 'ped' into ocuputiped;

	update redes r set ocupationenf = ocupenf where r.typerede = 1;
	update redes r set ocupationutiadul = ocuputiadul where r.typerede = 1;
	update redes r set ocupationutiped = ocuputiped where r.typerede = 1;
	update redes r set ocupationtotal = totalocupacoes where r.typerede = 1;

return new;
end
$$ language plpgsql;

--3 funcao atualiza leitos rede mista
create or replace function atualizaocupacoesmista() returns trigger as 
$$
declare
	regatual int;
	ocupenf int;
	ocuputiadul int;
	ocuputiped int;
	totalocupacoes int;
	
begin
	select count(idinternacao) from internacoes i where i.typerede = 2 into totalocupacoes;
	select count(idinternacao) from internacoes i where i.typerede = 2 and i.ocupation = 'enf' into ocupenf;
	select count(idinternacao) from internacoes i where i.typerede = 2 and i.ocupation = 'adul' into ocuputiadul;
	select count(idinternacao) from internacoes i where i.typerede = 2 and i.ocupation = 'ped' into ocuputiped;

	update redes r set ocupationenf = ocupenf where r.typerede = 2;
	update redes r set ocupationutiadul = ocuputiadul where r.typerede = 2;
	update redes r set ocupationutiped = ocuputiped where r.typerede = 2;
	update redes r set ocupationtotal = totalocupacoes where r.typerede = 2;

return new;
end
$$ language plpgsql;







--1 trigger atualiza leitos rede publica
create trigger triatualizaocupacoespublica 
after insert or delete or update on internacoes
for each row 
execute procedure atualizaocupacoespublica();

--2 trigger atualiza leitos rede publica
create trigger triatualizaocupacoesprivada 
after insert or delete or update on  internacoes
for each row 
execute procedure atualizaocupacoesprivada();

--3 trigger atualiza leitos mistos
create trigger triatualizaocupacoesmista 
after  insert or delete or update on internacoes
for each row 
execute procedure atualizaocupacoesmista();

--insercao de dados no banco de dados
-- os dados (tuplas) foram inspirados nos dados da tabela caso_full
-- os atributos que não existiam foram criados
-- se for um banco de dados novo, inicia o banco com todas as ocupações (de cada rede) vazias
-- typerede := 0,1,2 = rede publica, privada, mista
insert into redes values 
	(0,'public',0,0,0,0),
	(1,'private',0,0,0,0),
	(2,'mixed',0,0,0,0);

-- insercao de locais
insert into locais values ('city','AC','Acrelândia',1200013,15490);
insert into locais values ('city','AC','Assis brasil',1200054,7534);
insert into locais values ('city','AC','Brasiléia',1200104,26702);
insert into locais values ('city','AC','Bujari',1200138,10420);
insert into locais values ('city','AC','Capixaba',1200179,12008);
insert into locais values ('city','AC','Cruzeiro do Sul',1200203,89072);
insert into locais values ('city','AC','Epitaciolândia',1200252,18696);
insert into locais values ('city','AC','Feijó',1200302,34884);
insert into locais values ('city','AC','Jordão',1200328,8473);
insert into locais values ('city','AC','Mâncio Lima',1200336,19311);
insert into locais values ('city','AL','Paulo Jacinto',2706604,8473);
insert into locais values ('city','AL','Porto Calvo',2707305,19312);
-- deletando locais
delete from locais where city_ibge_code = 2707305;
insert into locais values ('city','AL','Porto Calvo',2707305,19312);

-- insercao de notificacoes
insert into notificacoes values (566190,202020,'2020-05-10',3076,8817.79612,1,59,0,0.0192,2706604);
insert into notificacoes values (927791,202020,'2020-05-10',503,5936.50419,5,2,0,0.004, 2707305);
insert into notificacoes values (225618,202122,'2021-06-04',1579,10193.67334,1,33,10,0.0209,1200013);
insert into notificacoes values (225619,202122,'2021-06-04',1579,10193.67334,1,33,10,0.0209,1200013);
insert into notificacoes values (546498,202122,'2021-06-04',1645,21834.35094,1,24,0,0.0146,1200054);
insert into notificacoes values (585923,202122,'2021-06-04',2272,11765.31511,1,28,0,0.0123,1200336);
insert into notificacoes values (427080,202122,'2021-06-04',2754,10313.83417,1,38,0,0.0138,1200104);
insert into notificacoes values (599255,202122,'2021-06-04',1119,10738.96353,1,17,0,0.0152,1200138);
insert into notificacoes values (235103,202122,'2021-06-04',638,5313.12458,1,17,0,0.0266,1200179);
insert into notificacoes values (500858,202122,'2021-06-04',7483,8401.0688,1,152,0,0.0203,1200203);
insert into notificacoes values (922723,202122,'2021-06-04',1370,7327.77065,1,29,0,0.0212,1200252);
insert into notificacoes values (566172,202122,'2021-06-04',3076,8817.79612,1,59,0,0.0192,1200302);
insert into notificacoes values (927747,202122,'2021-06-04',503,5936.50419,1,2,0,0.004, 1200328);
insert into notificacoes values (225620,202122,'2021-06-05',1579,10193.67334,1,33,1,0.0209,1200013);
insert into notificacoes values (225621,202122,'2021-06-05',1579,10193.67334,1,33,1,0.0209,1200013);
insert into notificacoes values (566173,202122,'2021-07-04',3076,8817.79612,1,59,0,0.0192,1200302);
insert into notificacoes values (927748,202122,'2021-07-04',503,5936.50419,5,2,0,0.004, 1200328);
insert into notificacoes values (927749,202122,'2021-07-04',503,5936.50419,5,2,0,0.004, 1200328);

-- insercao de internacoes
insert into internacoes values (85372,'sem observacoes',225618,1,'enf');
insert into internacoes values (85370,'sem observacoes',225619,1,'ped');
insert into internacoes values (41027,'paciente diabetico',546498,0,'enf');
insert into internacoes values (86437,'alergia a dipirona',427080,0,'enf');
insert into internacoes values (11705,'transferencia da rede publica',599255,1,'ped');
insert into internacoes values (71991,'sem observacoes',235103,0,'enf');
insert into internacoes values (13688,'',500858,1,'enf');
insert into internacoes values (15278,'morador da area rural',922723,0,'enf');
insert into internacoes values (92664,'acompanhar glicose alterada',566172,0,'enf');
insert into internacoes values (18643,'paciente com sonda urinaria, acompanhar quantidade urina',927747,0,'adul');
insert into internacoes values (69694,'transferido de outra localidade: monte alegre de minas',585923,2,'adul');
insert into internacoes values (18644,'',566190,0,'adul');
insert into internacoes values (69695,'',927791,2,'adul');
insert into internacoes values (18645,'',566173,0,'adul');
insert into internacoes values (18646,'',927748,0,'adul');
insert into internacoes values (69696,'',927749,0,'adul');
--manipulacao do banco de dados

--deletando e atulizando internacoes
delete from internacoes where idinternacao = 69695;
select * from redes;
insert into internacoes values (69695,'',927791,2,'adul'); --colocado novamente
select * from redes;

update internacoes set observations = 'paciente desenvolveu diabetes' where idinternacao = 18644;
update internacoes set ocupation = 'ped' where idinternacao = 18644;
select * from redes;
select * from internacoes;

--deletando notificacoes 
-- (não é interessante deletar uma notificaçao, mas foi colocado pra exemplificar a acao da trigger)

delete from notificacoes where idnotificacao = 927791; -- as internacoes relacionadas a esta notificacao sera excluida
select * from redes;

--inserindo novamente
insert into notificacoes values (927791,202020,'2020-05-10',503,5936.50419,5,2,0,0.004, 2707305);
insert into internacoes values (69695,'',927791,2,'adul');
select * from redes;

--visualizando todas as tabelas
select * from redes;
select * from locais;
select * from notificacoes;
select * from internacoes;

--especificacoes em consultas
--1 quais cidades obtiveram notificacoes com mais de 1 mortes por dia (mostrar as datas e as quantidades). 
-- verifica quais cidades tiveram mortes
select tab.* from (
	select l.city, n.date, sum(n.new_deaths) as deaths from notificacoes n join locais l on n.city_ibge_code =l.city_ibge_code
		group by n.date, l.city 
		) as tab
	where tab.deaths > 1;


--2 qual estado tem mais casos confirmados (mostrar a quantidade). 
select l.state, sum(n.new_confirmed) as cases from notificacoes n, locais l where n.city_ibge_code =l.city_ibge_code
	group by l.state
	order by cases desc
limit 1;

--3 total de casos confirmados em todo pais
select sum(n.new_confirmed) from notificacoes n;

--4 total de mortes confirmadas em todo pais
select sum(n.new_deaths) from notificacoes n;

--5 qual rede possui mais leitos ocupadas (mostrar a quantidade).
select r.nameRede, r.ocupationtotal from redes r
	order by r.ocupationtotal desc 
limit 1;

--6 qual rede possui mais leitos da utis pediatricas ocupadas (mostrar a quantidade)
select r.nameRede, r.ocupationenf from redes r
	order by r.ocupationenf desc 
limit 1;

--7 qual rede possui mais leitos da utis pediatricas ocupadas
select r.nameRede, r.ocupationutiped from redes r
	order by r.ocupationutiped desc 
limit 1;

--8 qual rede possui mais leitos da utis adulto ocupadas
select r.nameRede, r.ocupationutiadul from redes r
	order by r.ocupationutiadul desc 
limit 1;

--9 mostre a quantidade de notificações diárias para cada cidade
select n.date, l.city, count(n.date) as cases from notificacoes n join locais l on n.city_ibge_code =l.city_ibge_code 
	group by n.date, l.city
	order by n.date;

--10 mostre quais notificaçoes são referentes a internação nas enfermarias da rede publica
select n.* from notificacoes n join internacoes i on n.idnotificacao = i.idnotificacao 
	where i.ocupation = 'enf' and i.typerede = 0;

--11 mostre quais notificaçoes são referentes a internação nas utis pediatricas da rede privada
select n.* from notificacoes n join internacoes i on n.idnotificacao = i.idnotificacao 
	where i.ocupation = 'ped' and i.typerede = 1;

--12 mostre quais notificaçoes são referentes a internação nas utis adulto da rede mista
select n.* from notificacoes n join internacoes i on n.idnotificacao = i.idnotificacao 
	where i.ocupation = 'adul' and i.typerede = 2;

--13 mostrar a procentagem de leitos ocupados de cada rede e cada tipo de ocupacao 
--   em relacao ao total ocupacao de cada rede, e o total de leitos em cada rede
select r.nameRede, round((100*r.ocupationenf*r.ocupationtotal^(-1))::numeric,2) as "enfermaria (%)",
			 round((100*r.ocupationutiped*r.ocupationtotal^(-1))::numeric,2)  as "utiped (%)", 
			 round((100*r.ocupationutiadul*r.ocupationtotal^(-1))::numeric,2) as "utiadul (%)",
	 r.ocupationtotal
	from redes r where r.ocupationtotal!=0; 

--14 selecione as notificaçoes feitas nos dias 05/06/2021 e 04/07/2021. 
-- as datas podem ser algum feriado ou dia apos semana de feriado. saber se o feriado influenciou nos casos
select n.* from notificacoes n where n.date='2021-06-05'
union
select n.* from notificacoes n where n.date='2021-07-04';

--15 quais cidades obtiveram notificacoes com nenhuma morte por dia (mostrar as datas). 
-- poder saber se uma cidade nao teve nenhuma morte em algum dia
select tab.city, tab.date from (
	select l.city, n.date, sum(n.new_deaths) as deaths from notificacoes n join locais l on n.city_ibge_code =l.city_ibge_code
		group by n.date, l.city, n.city_ibge_code 
		) as tab
	where tab.deaths = 0
	order by tab.city;



-- cria uma visão com todas as informacoes dos casos registados
create or replace view visao_bd_radar_comhospi as
select l.city_ibge_code,l.city,l.state,l.place_type,l.estimated_population,
		n.idnotificacao,n.date,n.epidemiological_week,n.last_available_death_rate, 
		n.last_available_confirmed_per_100k_inhabitants,
		n.new_confirmed,n.new_deaths,n.total_confirmations,n.total_deaths, 
		i.idinternacao,i.observations,
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 0 and i1.ocupation = 'enf' and n1.date = n.date) as "publicenf",
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 0 and i1.ocupation = 'adul' and n1.date = n.date) as "publicutiadul",
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 0 and i1.ocupation = 'ped' and n1.date = n.date) as "publicutiped",		
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 0 and n1.date = n.date) as "publictotal" ,
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 1 and i1.ocupation = 'enf' and n1.date = n.date) as "enfprivate",
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 1 and i1.ocupation = 'adul' and n1.date = n.date) as "privateutiadul",
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 1 and i1.ocupation = 'ped' and n1.date = n.date) as "privateutiped",		
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 1 and n1.date = n.date) as "privatetotal" ,
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 2 and i1.ocupation = 'enf' and n1.date = n.date) as "mixedenf",
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 2 and i1.ocupation = 'adul' and n1.date = n.date) as "mixedutiadul",
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 2 and i1.ocupation = 'ped' and n1.date = n.date) as "mixedutiped",		
		(select count(i1.idinternacao) 
			from internacoes i1 join notificacoes n1 on i1.idnotificacao = n1.idnotificacao 
			where i1.typerede = 2 and n1.date = n.date) as "mixedtotal" 
	from redes r join internacoes i on r.typerede=i.typerede
		 right join notificacoes n on i.idnotificacao = n.idnotificacao 
			 join locais l on l.city_ibge_code = n.city_ibge_code;

select * from visao_bd_radar_comhospi;
			
--drop view visao_bd_radar_comhospi;
--drop trigger triatualizaocupacoesmista on internacoes;
--drop trigger triatualizaocupacoesprivada on internacoes;
--drop trigger triatualizaocupacoespublica on internacoes;
--drop function atualizaocupacoesmista();
--drop function atualizaocupacoesprivada();
--drop function atualizaocupacoespublica();
--drop table internacoes;
--drop table notificacoes;
--drop table locais;
--drop table redes;

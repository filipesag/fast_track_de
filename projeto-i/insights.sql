/*Qual a média de avaliação por estado?*/
select ROUND(AVG(f.score),2) as avg_score, c.customer_state 
from fact_order f left join dim_customer c 
on f.order_customer_id = c.customer_id 
where f.score != -1 
group by c.customer_state 
order by avg_score desc;


/*Qual o faturamento nos útimos anos?*/
select t.order_year, sum(f.payment_value) as max_payment_value
from fact_order f left join dim_time t
on f.order_time_id = t.order_time_id 
left join dim_order_status s
on f.order_status_id = s.status_id
where s.order_status not in ('canceled','unavailable','processing','invoiced')
group by t.order_year;


/*Qual o faturamento por estado nos útimos anos?*/
select SUM(f.payment_value) as max_value_paid, c.customer_state 
from fact_order f left join dim_customer c 
on f.order_customer_id = c.customer_id 
left join dim_order_status s 
on f.order_status_id = s.status_id
where s.order_status not in ('canceled','unavailable','processing','invoiced') 
group by c.customer_state 
order by max_value_paid desc;

/*Qual a taxa de cancelamento de pedidos em cada mês nos útimos anos?*/
select 
	count(f.order_id) as n_cancel_occurrances,
	ti.order_month as month,
	ti.order_year as year,
	c.customer_state as uf
from fact_order f left join dim_order_status t
on f.order_status_id = t.status_id
left join dim_time ti
on f.order_time_id = ti.order_time_id
left join dim_customer c
on f.order_customer_id = c.customer_id 
where t.order_status = 'canceled'
group by ti.order_month, ti.order_year, c.customer_state
order by  year asc, n_cancel_occurrances desc;

/*De onde vem as pessoas que nos deram nota menor que 3?*/
select 
	c.customer_id,
	c.customer_city,
	c.customer_state,
	f.score
from fact_order f left join dim_customer c
on f.order_customer_id = c.customer_id
where f.score < 3 and  f.score != -1;


/*
Análise de Vendas por Categoria e Região (com Variação Mensal):
"Liste o valor total de vendas (payment_value), o número total de itens (number_of_items) e a 
média da avaliação (score) para cada combinação de product_category e customer_state para o ano de 2018. 
Além disso, para cada categoria/estado, calcule a variação percentual do payment_value em relação ao mês anterior (dentro do mesmo ano e categoria/estado). 
Inclua apenas as categorias de produtos que tiveram mais de 100 vendas no total em 2018."
*/

with cte_monthly_order as (
	select 
		t.order_month as month,
		d.product_category as category,
		c.customer_state as uf,
		sum(f.payment_value) as payment_total,
		sum(f.number_of_items) as total_items,
		ROUND(avg(f.score),2) as avg_score
	from fact_order f
	left join dim_product d 
	on f.order_product_id = d.product_id
	left join dim_time t
	on f.order_time_id = t.order_time_id
	left join dim_customer c 
	on f.order_customer_id = c.customer_id 
	where f.score != -1 and t.order_year = '2018'
	group by  c.customer_state,t.order_month, d.product_category
), cte_percentage_rate as (
	select
		uf,
		month,
		category,
		payment_total,
		total_items,
		avg_score,
		LAG(payment_total) OVER(partition by uf,category,month order by uf) as previous_payment
	from cte_monthly_order
)


/*Performance de Pagamento e Avaliação por Método e Trimestre:
"Identifique, para cada payment_method e order_trimester no ano de 2017, a soma do payment_value e a média do score das ordens. 
Em seguida, encontre os top 3 payment_methods em termos de payment_value para cada trimestre. Considere apenas ordens com score maior que 0."
*/

with cte_payment_perfomance as (
	select 
		p.payment_method,
		t.order_trimester,
		sum(f.payment_value) as payment_total,
		round(avg(f.score),2) as avg_score
	from fact_order f left join dim_payment_method p
	on f.order_payment_method_id = p.payment_method_id
	left join dim_time t
	on f.order_time_id = t.order_time_id
	where t.order_year = '2017' and f.score != -1
	group by p.payment_method, t.order_trimester
	order by t.order_trimester
), cte_rank_pay_method as (
	select 
		*,
		RANK() OVER(partition by order_trimester order by payment_total desc) as rank
	from cte_payment_perfomance
)

select *
from cte_rank_pay_method
where rank in (1,2,3);


/*Determine a média de score de cada forma de pagamento em cada ano*/
with cte as (
	select 
		p.payment_method,
		t.order_year as year,
		round(avg(f.score),2) as avg_score
	from fact_order f
	left join dim_payment_method p
	on f.order_payment_method_id = p.payment_method_id
	left join dim_time t
	on f.order_time_id = t.order_time_id
	where score != -1 and p.payment_method != 'not_defined'
	group by p.payment_method, t.order_year
	order by year 
)

select * from cte;
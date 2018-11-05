CREATE TABLE `sales`.`sales_2016` (
  `sale_date` VARCHAR(45) NULL,
  `item_name` VARCHAR(45) NULL,
  `net_quantity` INT NULL,
  `index_id` INT NOT NULL,
  PRIMARY KEY (`index_id`));

CREATE TABLE `sales`.`temperature_master` (
  `date` VARCHAR(45) NULL,
  `avg_sf_temperature` INT NULL,
  `index_id` INT NOT NULL,
  PRIMARY KEY (`index_id`));

# Report 1
select temperature_master.avg_sf_temperature, sales_2016.item_name, sum(sales_2016.net_quantity)
from sales_2016 join temperature_master on sales_2016.sale_date = temperature_master.date
group by sales_2016.item_name 
order by sum(sales_2016.net_quantity) desc, sales_2016.item_name;

# Create Analytics View for reports
create view analytics_view as (    
	select temperature_master.index_id,  temperature_master.date, 
	temperature_master.avg_sf_temperature, sales_2016.item_name, sales_2016.net_quantity
	from sales_2016 join temperature_master on sales_2016.sale_date = temperature_master.date
	order by temperature_master.date
);

# Report 2
select warmer.item_name, warmer.avg_change_warmer, colder.avg_change_colder
from (
select result.item_name, avg(result.order_change) as avg_change_warmer  from (
select a1.date, a1.item_name, a1.net_quantity, (a2.avg_sf_temperature - a1.avg_sf_temperature) as sub1, (a2.net_quantity - a1.net_quantity) as order_change
from analytics_view a1, analytics_view a2
where a2.index_id - a1.index_id = 5) as result 
where result.sub1 = 2
group by result.item_name
order by avg_change_warmer desc) as warmer
join  (
select result.item_name, avg(result.order_change) as avg_change_colder from (
select a1.date, a1.item_name, a1.net_quantity, (a2.avg_sf_temperature - a1.avg_sf_temperature) as sub1, (a2.net_quantity - a1.net_quantity) as order_change
from analytics_view a1, analytics_view a2
where a2.index_id - a1.index_id = 5) as result 
where result.sub1 = -2
group by result.item_name
order by avg_change_colder  desc) as colder
on warmer.item_name = colder.item_name;
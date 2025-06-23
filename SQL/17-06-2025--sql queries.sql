#Question 1: Find Employees Earning More Than Their Department’s Average Salary
#==============================================================================
select e.emp_id, e.dept_id, e.salary
from Employees e
join Departments d on e.dept_id = d.dept_id 
join (
  select dept_id, avg(salary) as avg_salary
  from Employees
  group by dept_id
) da on e.dept_id = da.dept_id
where e.salary > da.avg_salary;

#Question 2: Find Employees Who Worked on All Projects in Their Department
#=========================================================================
SELECT e.emp_id, e.emp_name
FROM Employees e
JOIN EmployeeProjects ep ON e.emp_id = ep.emp_id
JOIN Projects p ON ep.project_id = p.project_id
WHERE p.dept_id = e.dept_id
GROUP BY e.emp_id, e.emp_name, e.dept_id
HAVING COUNT(p.project_id) = (
    SELECT COUNT(*)
    FROM Projects p2
    WHERE p2.dept_id = e.dept_id
);

#Question 3: Find the Highest-Paid Employee in Each Department Who Started After 2020
#=====================================================================================
select e.emp_id, e.emp_name, e.dept_id, e.salary, e.hire_date, d.dept_name
from employees e
join departments d on e.dept_id = d.dept_id
where e.salary = (
    select MAX(salary)
    from Employees
    where hire_date > '2020-01-01'
);

#Question 4: Find Departments Where All Employees Earn Above a Certain Threshold
#===============================================================================
select *
from Departments d
join Employees e on d.dept_id = e.dept_id
group by d.dept_id, d.dept_name
having MIN(e.salary) > 50000;

#Question 1: Find Customers Whose Total Order Amount Exceeds the Average Order Amount in Their Region
#=====================================================================================================
select c.customer_name,region 
from order_details od 
inner join orders o on od.order_id=o.order_id 
join customers c on o.customer_id =c.customer_id
where o.total_amount>(
 select avg(o1.total_amount)
 from customers c1 join orders o1 on c1.customer_id=o1.customer_id 
 where c1.region=c.region)  
 group by c.customer_name;
 
#Question 2: Find Orders Containing Products from All Categories
#================================================================
select od.order_id, od.product_id,od.quantity
from order_details od
join products p on od.product_id = p.product_id
group by od.order_id
having COUNT(p.category) = (
    select COUNT(distinct category) from products
);

#Question 3: Find the Most Expensive Product Ordered by Each Customer After a Specific Date
#==========================================================================================
select o.customer_id,o.order_date,od.product_id,
    MAX(od.unit_price) as max_price
from orders o
join order_details od on o.order_id = od.order_id
where o.order_date > '2023-01-31'
group by o.customer_id;

#Question 4: Find Regions Where No Customer Ordered a Specific Product
 #====================================================================
select region
from customers
where region  in (
    select region
    from customers c
    join orders o on c.customer_id = o.customer_id
    join order_details od on o.order_id = od.order_id
    join products p on od.product_id = p.product_id
    where p.product_name = 'Laptop'
);
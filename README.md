#  Pyspark_assignment

# 1. Question:
# 1.1.Create DataFrame as purchase_data_df, product_data_df with custom schema with the below data
<img width="950" height="944" alt="Screenshot 2025-12-07 124341" src="https://github.com/user-attachments/assets/f3dcb415-e01a-4453-af1b-23b60714fdc6" />

<img width="868" height="600" alt="Screenshot 2025-12-07 124515" src="https://github.com/user-attachments/assets/9d75f393-56bf-4478-9bd8-b24988653ce9" />


# 1.2.Find the customers who have bought only iphone13

<img width="641" height="418" alt="Screenshot 2025-12-07 134122" src="https://github.com/user-attachments/assets/ba62cb83-ff55-40d0-b8f3-10d7d8fa5476" />

# 1.3.Find customers who upgraded from product iphone13 to product iphone14
<img width="910" height="319" alt="Screenshot 2025-12-07 134527" src="https://github.com/user-attachments/assets/fe6b766e-da37-48b9-be6b-595506e33249" />


# 1.4.Find customers who have bought all models in the new Product Data
<img width="733" height="331" alt="Screenshot 2025-12-07 135134" src="https://github.com/user-attachments/assets/df30a02e-b984-4255-8d59-95f11ebecaa6" />


# 2.Question:
<img width="495" height="474" alt="Screenshot 2025-12-07 135830" src="https://github.com/user-attachments/assets/88c9d15a-4a88-4d26-9455-7407ec05eee9" />


# 2.1. print number of partitions


<img width="414" height="253" alt="Screenshot 2025-12-07 142118" src="https://github.com/user-attachments/assets/f153b263-923b-4dc6-8def-08f9e83b620c" />



# 2.2 Increase the partition size to 5 

<img width="660" height="166" alt="Screenshot 2025-12-07 142750" src="https://github.com/user-attachments/assets/b0769db9-3a95-48f7-b9b1-8289b5d1300d" />


# 2.3 Decrease the partition size back to its original partition size 

<img width="693" height="101" alt="image" src="https://github.com/user-attachments/assets/94cd925a-6e4f-4f73-9462-56b3e5a20000" />

<img width="647" height="47" alt="image" src="https://github.com/user-attachments/assets/8a382e79-4fa6-4950-85e5-12d2838504de" />



# 2.4Create a UDF to print only the last 4 digits marking the remaining digits as * 

Eg: ************4567 



<img width="678" height="117" alt="Screenshot 2025-12-07 142809" src="https://github.com/user-attachments/assets/81430f62-611b-42dd-8a60-f51b7ea0fd7f" />




# 2.5 output should have 2 columns as card_number, masked_card_number(with output of question 2) 

<img width="986" height="406" alt="Screenshot 2025-12-07 142833" src="https://github.com/user-attachments/assets/64d0afcd-ff00-43b1-99f9-95f4fe05b381" />


# 3. Question

# 3.1. Create a Data Frame with custom schema creation by using Struct Type and Struct Field Column names: log id, user$id, action, and timestamp.

<img width="591" height="797" alt="Screenshot 2025-12-14 212840" src="https://github.com/user-attachments/assets/4fc152a3-361b-478f-82d4-cd85412023f4" />



# 3.2.Column names should be log_id, user_id, user_activity, time_stamp using dynamic function
<img width="711" height="430" alt="Screenshot 2025-12-14 213150" src="https://github.com/user-attachments/assets/89420ff2-72d4-42da-aad0-2cdad89b01d6" />


# 3.3. Write a query to calculate the number of actions performed by each user in the last 7 days
<img width="727" height="332" alt="Screenshot 2025-12-14 215043" src="https://github.com/user-attachments/assets/eba14a68-2170-422a-bb79-58296f616409" />


# 3.4. Convert the time stamp column to the login_date column with YYYY-MM-DD format with date type as its data type
<img width="587" height="166" alt="Screenshot 2025-12-14 223019" src="https://github.com/user-attachments/assets/d5df3815-82d8-46f7-b4c9-b42fc3e2b1ea" />
<img width="540" height="224" alt="Screenshot 2025-12-14 223040" src="https://github.com/user-attachments/assets/0692339f-cc3e-45e5-8131-bbff484e4e14" />


# 4. Question
# 4.1. Read JSON file provided in the attachment using the dynamic function
<img width="886" height="353" alt="Screenshot 2025-12-14 224816" src="https://github.com/user-attachments/assets/88ae5e37-6f02-41a3-adb4-e25ebaf39430" />


# 4.2. flatten the data frame which is a custom schema

<img width="790" height="297" alt="Screenshot 2025-12-14 225903" src="https://github.com/user-attachments/assets/33dd7daf-d9e4-443e-93fb-f2c74b710ee4" />

# 4.3. find out the record count when flattened and when it's not flattened(find out the difference why you are getting more count)

<img width="505" height="246" alt="Screenshot 2025-12-14 230403" src="https://github.com/user-attachments/assets/40e3c66e-1ab0-4e36-916b-d29659f23c8b" />

# 4.4. Differentiate the difference using explode, explode outerfunctions

<img width="750" height="507" alt="Screenshot 2025-12-14 230936" src="https://github.com/user-attachments/assets/24a727e6-d7c5-4ff1-a1d9-8a48e899e9a7" />
<img width="831" height="151" alt="Screenshot 2025-12-14 230944" src="https://github.com/user-attachments/assets/4b8212cb-7650-4646-aec5-f56cb25b81db" />


# 4.5. Filter the id which is equal to 0001 

<img width="796" height="450" alt="Screenshot 2025-12-14 231128" src="https://github.com/user-attachments/assets/8faaec65-4bfb-4bf5-bc79-66784e478f27" />

# 4.6. Add a new column named load_date with the current date

<img width="736" height="381" alt="Screenshot 2025-12-21 115404" src="https://github.com/user-attachments/assets/deaea18d-f456-45b0-941c-e6cc9a88b8c5" />


# 4.7. create 3 new columns as year, month, and day from the load_date column

<img width="675" height="356" alt="Screenshot 2025-12-21 115417" src="https://github.com/user-attachments/assets/f0d61042-101a-46cb-a316-c85fb66d0b70" />


# 5.1. create all 3 data frames as employee_df, department_df, country_df with custom schema defined in dynamic way

<img width="857" height="537" alt="Screenshot 2025-12-21 120105" src="https://github.com/user-attachments/assets/77115bbd-014f-4677-84af-af337772f419" />

<img width="712" height="327" alt="Screenshot 2025-12-21 120121" src="https://github.com/user-attachments/assets/308aa5c3-b042-4df3-9fdf-707487a1c615" />

<img width="784" height="431" alt="Screenshot 2025-12-21 120141" src="https://github.com/user-attachments/assets/e17eb113-2267-4fd8-bec9-515c78100b26" />

<img width="571" height="642" alt="Screenshot 2025-12-21 120204" src="https://github.com/user-attachments/assets/c9c63c78-3296-40bf-9785-78f5a08d9114" />


# 5.2. Find avg salary of each department
<img width="927" height="267" alt="image" src="https://github.com/user-attachments/assets/b490b4de-caf0-4dcf-84c6-2f9613e3d7c3" />


# 5.3. Find the employee’s name and department name whose name starts with ‘m’

<img width="655" height="414" alt="Screenshot 2025-12-21 121458" src="https://github.com/user-attachments/assets/4fb7f2dc-c908-467e-93ae-8ff56ac8eef4" />



# 5.4. Create another new column in employee_df as a bonus by multiplying employee salary *2

<img width="681" height="403" alt="Screenshot 2025-12-21 121522" src="https://github.com/user-attachments/assets/e7b2f4d4-b848-475c-a0bf-859552c20a3f" />


# 5.5. Reorder the column names of employee_df columns as (employee_id,employee_name,salary,State,Age,department)

<img width="993" height="384" alt="Screenshot 2025-12-21 122530" src="https://github.com/user-attachments/assets/135bf2a9-642c-4731-bce7-b337cdbdb500" />


# 5.6. Give the result of an inner join, left join, and right join when joining employee_df with department_df in a dynamic way
<img width="927" height="1030" alt="Screenshot 2025-12-21 123000" src="https://github.com/user-attachments/assets/ac1204bc-786b-45a7-b94d-794a2f229d36" />



# 5.7. Derive a new data frame with country_name instead of State in employee_df
Eg(11,“james”,”D101”,”newyork”,8900,32)

<img width="1167" height="414" alt="Screenshot 2025-12-21 123407" src="https://github.com/user-attachments/assets/8609a16c-94d6-4ba0-929a-8ee382624261" />


# 5.8. convert all the column names into lowercase from the result of question 7in a dynamic way, add the load_date column with the current date

<img width="1310" height="372" alt="image" src="https://github.com/user-attachments/assets/047fc72a-a480-4aec-ad54-081cbaa7caae" />


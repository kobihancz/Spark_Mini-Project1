# TO DO: the pyspark import is not being recognized FIXED
# TO DO: get it to run in the terminal FIXED

#to run use command "spark-submit Main_Spark.py" in the Spark_Mini-Project Directory 

from pyspark import SparkConf, SparkContext

#create entry point into spark 
conf = SparkConf().setMaster("local").setAppName("AutoPostSalesRep")
sc = SparkContext(conf=conf)

#create an rdd from given data file 
raw_rdd = sc.textFile("data.csv")

#function to iterate through data and return a key value pair for each row
def extract_vin_key_value(row):
    split_row = row.split(",")
    vin = split_row[2]
    make = split_row[3]
    year = split_row[5]
    incident_type = split_row[1]

    return vin, (make, year, incident_type)

# this command places the above^ map function in the rdd
vin_kv = raw_rdd.map(lambda x: extract_vin_key_value(x))

#function to iterate through data and return a list of tuples
def populate_make(row):

    populated_list = []

    # iterate through each row
    for val in row:
        # filter the row and append to list if the make and year values are not empty
        if val[0].strip() != '':
            make = val[0]
        if val[1].strip() != '':
            year = val[1]
        incident_type = val[2]
        populated_list.append((make,year,incident_type))

    return populated_list

enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

# this function goes through the populated list and returns the make-year, 1 if they are an accident record 
def extract_make_key_value(auto_list):
    if auto_list[2] == 'A':
        formatted_data = str(auto_list[0]) + '-' + str(auto_list[1])
        return formatted_data, 1
    else:
        formatted_data = str(auto_list[0]) + '-' + str(auto_list[1])
        return formatted_data, 0


make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

enhance_make = make_kv.reduceByKey(lambda x, y: x + y)
print(enhance_make.collect())

#collect final colution
final_rdd = enhance_make.collect()

# write solution to text file
with open("final_solution.txt", "w") as f:
    for item in final_rdd:
        print(item)
        f.write(str(item)+ "\n")

#stop SparkContext applicationte
sc.stop()



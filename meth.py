import sys
from pyspark.sql import SparkSession

#Spark session Builder
spark = SparkSession\
        .builder\
        .appName("Python_Wind_Map")\
        .getOrCreate()

# Function of Calculation of New Wind Speed
def updatenewwind(w):
        x = int(w[0])
        y = int(w[1])
        z = (w[2])
        zprev  = (getnewz(updlist, (x-1), y)) # Gets Z(x-1,y)
        znext = (getnewz(updlist, (x+1), y)) # Gets Z(x+1,y)
        if z is not None: # If else if statements to calculate new Z from first record to last record in the RDD Stream
                if zprev is not None and znext is not None:
                        znew = alpha*float(zprev) + (1-2*alpha)*float(z) + alpha*float(znext)
                elif zprev is None and znext is not None:
                        znew = (1-alpha)*float(z) + alpha*float(znext)
                elif zprev is not None and znext is None:
                        znew = alpha*float(zprev) + (1-alpha)*float(z)
                else:
                        znew = float(z)
        else: z = 0
        return [x,y,znew]

# Function to Get Z value for calculation above
def getnewz(thislist,x,y):
        for i in thislist:
                if int(i[0]) == x and int(i[1]) == y:
                        return i[2]
        return None
# Printing function for easy viewing
def printing(printlist, j, nl, fl):
        of = "outputs/alpha1/out_%i.txt" %j # Output of [x y znew] used later for heatmap
        pf = "outputs/alpha1/outalpha1.txt" # Output of number of Nulls and Filtered values in each iteration.
        f= open(of, "w")
        pff = open(pf, "a")
        pff.write(" The number of null values in out_%i.txt %i\n" %(j ,nl))
        pff.write(" The number of filtered values in out_%i.txt %i\n" %(j, fl))
        for r in printlist:
                f.write("%i, %i, %f" %(r[0], r[1], r[2]) +"\n")
        f.close()
        pff.close()

# Function to Replace Null/None Z with a value(99999) out of scope of Z = (-115,115)		
def repnullwz(w):
        x = int(w[0])
        y = int(w[1])
        z = (w[2])
        if z == 'None':
                z = unicode('99999', "utf-8")
        else: z == z
        return [x,y,z]

alpha = 0.1  # Alpha Value constant 0.1, ran the code 4 times with 4 alpha values
itr = int(sys.argv[2]) #Taking Argument 2 as number of iterations, here 50 iterations.
inputfile = spark.read.text(sys.argv[1]).rdd.map(lambda w: w[0]).map(lambda w: w.split(", ")) # file read
rfile = inputfile
ifile = rfile.map(repnullwz) #function calling replace null with z values, assigning null Z to 99999, to count later.
for j in range(itr):
        nlist = ifile.filter(lambda x: float(x[2]) == 99999) # Counting number of Null Values.
        nl =  len(nlist.collect()) #Storing null values to nlist.(nl)
        flist = ifile.filter(lambda x: float(x[2]) > -100 and float(x[2]) < 100 or float(x[2]) == 99999) #filtering all values between -100 and 100, including nulls.
        fl = len(flist.collect()) # To check the number of filtered values.
        updlist = flist.collect() #Storing filtered values in flist(fl)
        mlist = flist.map(updatenewwind) # Calculating for new Z
        printing(mlist.collect(), j, nl, fl) # Printing function for Easy Viewing of output and using output file to generate heatmap in local
        ifile = mlist #For next iteration
spark.stop()

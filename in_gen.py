import random # importing random to generate random values of Z(wind speed)
f = open("test.txt", "w")
for x in range(1,51): # range taken for X values
        for y in range(1,51): # range taken for Y values
                i = random.randint(0,50) # Assigning randomly 1 in 50 values as 'null' or 'none'
                if (i==5):
                        z = None
                        f.write("%i, %i, %s\n" %(x, y, z))
                else:
                        z = random.randint(-115,115) # Range of Z values(wind speed)
                        f.write("%i, %i, %i\n" % (x, y, z))
f.close()

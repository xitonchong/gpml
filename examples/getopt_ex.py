import sys
import getopt
  
def full_name():
    first_name = None
    last_name = None
  
    argv = sys.argv[1:]
  
    try:
        opts, args = getopt.getopt(argv, "f:l:", 
                                   ["first_name=", # cannot have space
                                    "last_name="])
      
    except:
        print("Error")
  
    for opt, arg in opts:
        print(opt, arg)
        if opt in ['-f', '--first_name']:
            first_name = arg
        elif opt in ['-l', '--last_name']:
            last_name = arg
      
  
    print(first_name, last_name)
  
full_name()
'''python getopt_ex.py -f usagi -l chong'''
'''python getopt_ex.py --first_name usagi --last_name chong'''
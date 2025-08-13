import argparse

parser = argparse.ArgumentParser(description="Command that lanch the landing program.") 
parser.add_argument('--flux_name' , type=str, required=True)
parser.add_argument('--country' , type=str, required=True)
parser.add_argument('--vertical' , type=str, required=True)
parser.add_argument('--system' , type=str, required=True)

args = parser.parse_args()
print('hello world :  ' , {args.flux_name})
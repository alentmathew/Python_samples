var = {
"Version": "2012-10-17",
"Statement": [
{
"Action": "ec2:*",
"Resource": "*",
"Effect": "Allow",
"Condition": {
"StringEquals": {
"ec2:Region": "us-east-2"
}
}
}
]
}


print(var)
print(var['Statement'])
print(var['Statement'][0])
print(var['Statement'][0]["Condition"])
print(var['Statement'][0]["Condition"]["StringEquals"])
print(var['Statement'][0]["Condition"]["StringEquals"]["ec2:Region"])


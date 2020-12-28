for i in {1..10}  
do  
go test -run FailAgree
done 
for i in {1..10}  
do  
go test -run FailNoAgree
done 
for i in {1..10}  
do  
go test -run ConcurrentStarts
done 
for i in {1..10}  
do  
go test -run Backup
done 
for i in {1..10}  
do  
go test -run Rejoin
done 
go test -run Persist1
go test -run Persist2
go test -run Persist3
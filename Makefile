PORT := $(word 2,$(MAKECMDGOALS))

proto: 
	python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. mapreduce.proto

make del:
	del /q dump\* output\* final-output\* output\*

coord:
	py coordinator.py

work:
	py worker.py $(PORT)

eq:
	py tools/equal.py

single:
	py tools/manual-single.py

multi:
	py tools/manual-multi.py

merge:
	py tools/merge.py

bigshake:
	if exist shakespeare_16.txt del shakespeare_16.txt
	for /L %%i in (1,1,16) do type shakespeare.txt >> shakespeare_16.txt
%:

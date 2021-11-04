build:
	# Compiling C files
	mpicc -c tasks.c
	mpicc -c utils.c
	# =================
	# Compiling C++ main() file, linking C-object files
	mpic++ -o a03 main.cpp tasks.o utils.o
	# =================
	# Copying over the executable to the remote host
	# Make sure that ssh-key has been set up between <CURR_HOST> and <TARGET_HOST_IN_NEXT_LINE>
	scp a03 e0323258@soctf-pdc-009:~/Ass3
	# ===== Done! =====
	# To run:
	# mpirun -np 10 -hostfile machinefile.1 ./a03 sample_input_files 6 8 2 test1.output 1

build_c:
	mpicc -o a03 main.c tasks.c utils.c

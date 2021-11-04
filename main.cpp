#define DEBUG_WORK_DISTRIBUTION 0
#define DEBUG_MAP_WORKER 1
#define DEBUG_MASTER 0
#define DEBUG_REDUCE_WORKER 0

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <vector>
extern "C" {
#include "tasks.h"
#include "utils.h"
}

std::string resolveFilePath(const char* input_files_dir, int i) {
    std::string path(input_files_dir);
    path.append("/");
    path.append(std::to_string(i));
    path.append(".txt");
    return path;
}

char* readFileIntoCharBuffer(const char* pathToRead, int* size) {
    FILE *f = fopen(pathToRead, "rb");
    fseek(f, 0, SEEK_END);
    long fsize = ftell(f);
    fseek(f, 0, SEEK_SET);  /* same as rewind(f); */
    char *targetBuffer = (char*) malloc(fsize + 1);
    fread(targetBuffer, 1, fsize, f);
    fclose(f);
    targetBuffer[fsize] = 0;
    *size = fsize + 1;
    return targetBuffer;
}


int main(int argc, char** argv) {
    constexpr int MASTER_RANK = 0;
    constexpr int SIZE_TAG = 0;
    constexpr int FILE_TAG = 1;
    MPI_Init(&argc, &argv);

    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    // Get command-line params
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    // Identify the specific map function to use
    MapTaskOutput* (*map) (char*);
    switch(map_reduce_task_num){
        case 1:
            map = &map1;
            break;
        case 2:
            map = &map2;
            break;
        case 3:
            map = &map3;
            break;
    }

    /*
    
    Create the work-distribution in the mapping phase.
    Round robin

    */
    int mapWorkerFilesToProcess[num_map_workers] = {0};
    for (int i = 0; i < num_files; i++) {
        mapWorkerFilesToProcess[i % num_map_workers] += 1;
    }

#if DEBUG_WORK_DISTRIBUTION
    if (rank == MASTER_RANK) {
        for (int i = 0; i < num_map_workers; i++)
            std::cout << "Rank (" << i + 1 << ") MapWorker will process " << mapWorkerFilesToProcess[i] << " files\n";
    }
#endif
    // Distinguish between master, map workers and reduce workers
    if (rank == MASTER_RANK) {
        /* Read the input files, and send them over to each Map worker. */

        /* Need to free all these char* */
        char* filePtrs[num_files];
        
        /* Size includes null char. */
        int sizes[num_files];

        MPI_Request fileReqs[num_files];
        MPI_Request sizeReqs[num_files];

        for (int i = 0; i < num_files; i++) {
            int currFileSize = -1;
            std::string filePath = resolveFilePath(input_files_dir, i);
            char* currFileContents = readFileIntoCharBuffer(filePath.c_str(), &currFileSize);
            filePtrs[i] = currFileContents;
            sizes[i] = currFileSize;

            int workerToSendTo = (i % num_map_workers) + 1;
            int sizeTag = i * 2;
            int fileTag = i * 2 + 1;

#if DEBUG_MASTER
            std::cout << "Rank (MASTER) Sending File " << filePath << " to MapWorker Rank = " << workerToSendTo << ", SizeTag = " << sizeTag << ", FileTag = " << fileTag << std::endl;
#endif
            MPI_Send(&currFileSize, 1, MPI_INT, workerToSendTo, sizeTag, MPI_COMM_WORLD); // size
            MPI_Send(currFileContents, currFileSize, MPI_CHAR, workerToSendTo, fileTag, MPI_COMM_WORLD); // actual file
        }
#if DEBUG_MASTER
        printf("Rank (MASTER): Master is Done\n");
#endif
    } else if ((rank >= 1) && (rank <= num_map_workers)) {
        
        int mapWorkerIdx = rank - 1;
        int fileIdxToExpect = mapWorkerIdx;
        int temp = mapWorkerFilesToProcess[mapWorkerIdx];
        /* While this map worker still has stuff to process. */
        while (temp > 0) {
            int sizeTagToExpect = fileIdxToExpect * 2;
            int fileTagToExpect = fileIdxToExpect * 2 + 1;
            //std::cout << "Rank (" << rank << "), Expecting File " << fileIdxToExpect << ", SizeTag = " << sizeTagToExpect << ", FileTag = " << fileTagToExpect << std::endl;
            int size;
            char* fileContent;
            // MPI_Request fileReq;
            // MPI_Request sizeReq;
            MPI_Status sizeStatus;
            MPI_Status fileStatus;
        
            MPI_Recv(&size, 1, MPI_INT, MASTER_RANK, sizeTagToExpect, MPI_COMM_WORLD, &sizeStatus);
            
            /* Need to free */
            fileContent = (char*) malloc(size);

            MPI_Recv(fileContent, size, MPI_CHAR, MASTER_RANK, fileTagToExpect, MPI_COMM_WORLD, &fileStatus);
#if DEBUG_MAP_WORKER
            printf("Rank (%d): Map Worker successfully received file contents\n", rank);
            printf("Rank (%d): Map Worker is working on Mapping File [%d.txt] \n", rank, fileIdxToExpect);
#endif
            /* Do the actual Mapping */
            MapTaskOutput* res = map(fileContent);
            free_map_task_output(res);

#if DEBUG_MAP_WORKER
            printf("Rank (%d): Map Worker is done Mapping File [%d.txt] \n", rank, fileIdxToExpect);
#endif
            fileIdxToExpect += num_map_workers;
            temp--;
        }
#if DEBUG_MAP_WORKER
        printf("Rank (%d): Map Worker is Done\n", rank);
#endif        
    } else {
        // TODO: Implement reduce worker process logic

#if DEBUG_REDUCE_WORKER
        printf("Rank (%d): This is a reduce worker process\n", rank);
#endif
    }

    //Clean up
    MPI_Finalize();
    return 0;
}

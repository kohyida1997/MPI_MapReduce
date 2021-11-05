#define DEBUG_WORK_DISTRIBUTION 0
#define DEBUG_MAP_WORKER 1
#define DEBUG_MASTER 0
#define DEBUG_REDUCE_WORKER 1

#define WORD_MAX_LENGTH 8
#define TAG_COMM_PAIR_LIST 50 // Fix reuse

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <vector>
#include <fstream>
#include <unordered_map>
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

 MPI_Status status;

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

    // --------- DEFINE THE STRUCT DATA TYPE TO SEND // modified from
    // https://github.com/adhithadias/map-reduce-word-count-openmp-mpi/blob/main/mpi_parallel.c 
    const int nfields = 2;
    MPI_Aint disps[nfields];
    int blocklens[] = {WORD_MAX_LENGTH, 1};
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};

    disps[0] = offsetof(KeyValue, key);
    disps[1] = offsetof(KeyValue, val);

    MPI_Datatype istruct;
    MPI_Type_create_struct(nfields, blocklens, disps, types, &istruct);
    MPI_Type_commit(&istruct);
    // ---

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

        /* Receive from reduce workers and write to file */
        std::ofstream myfile;
        myfile.open (output_file_name);

        for (int reduceWorkerIdx = 0; reduceWorkerIdx < num_reduce_workers; reduceWorkerIdx++) {
            int sizeOf = 0;
            int incomingKVSize = 26; // fix hardcoded res->len
            KeyValue incomingKVs[incomingKVSize]; 
            MPI_Recv(incomingKVs, incomingKVSize, istruct, MPI_ANY_SOURCE, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, istruct, &sizeOf);
            for (int i = 0; i < sizeOf; i++) {
                KeyValue kv = (incomingKVs)[i];
                myfile << kv.key << " " << kv.val << std::endl;
            }
        }
        myfile.close();

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

            for (int reduceWorkerIdx = 0; reduceWorkerIdx < num_reduce_workers; reduceWorkerIdx++) {
                KeyValue outgoingKVs[res->len];
                int sizeOf = 0;
                for (int i = 0; i < res->len; i++) {
                    if (i % num_reduce_workers != reduceWorkerIdx) {
                        continue;
                    }
                    KeyValue kv = (res -> kvs)[i];
                    outgoingKVs[sizeOf].val = kv.val;
                    strcpy(outgoingKVs[sizeOf].key, kv.key);
                    sizeOf++;
                }
                int reduceWorkerRank = reduceWorkerIdx + 1 + num_map_workers;
                MPI_Send(outgoingKVs, sizeOf, istruct, reduceWorkerRank, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD);
            }

#if DEBUG_MAP_WORKER
            printf("Rank (%d): Map Worker is done Mapping File [%d.txt] \n", rank, fileIdxToExpect);
#endif
            free_map_task_output(res);
            fileIdxToExpect += num_map_workers;
            temp--;
        }

        /* 
        Send sentinal value to reduce worker to indicate that this map worker is done
        */
        for (int reduceWorkerIdx = 0; reduceWorkerIdx < num_reduce_workers; reduceWorkerIdx++) {
            KeyValue outgoingKVs[1];
            int sizeOf = 0;
            outgoingKVs[sizeOf].val = -1;
            strcpy(outgoingKVs[sizeOf].key, "123");
            sizeOf++;
            int reduceWorkerRank = reduceWorkerIdx + 1 + num_map_workers;
            MPI_Send(outgoingKVs, sizeOf, istruct, reduceWorkerRank, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD);
        }

#if DEBUG_MAP_WORKER
        printf("Rank (%d): Map Worker is Done\n", rank);
#endif        
    } else {

        // buffer to hold KeyValues from all map workers
        std::unordered_map<std::string, std::vector<int>> buffer;

        int reduceWorkerIdx = rank - num_map_workers - 1;
        int mapWorkersDone = 0;

        // reducer keeps receiving values till all map workers are done
        while (mapWorkersDone < num_map_workers) {
            int sizeOf = 0;
            int incomingKVSize = 26; // fix hardcoded res->len
            KeyValue incomingKVs[incomingKVSize]; 
            MPI_Recv(incomingKVs, incomingKVSize, istruct, MPI_ANY_SOURCE, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, istruct, &sizeOf);
            for (int i = 0; i < sizeOf; i++) {
                KeyValue kv = (incomingKVs)[i];
                if (kv.val == -1) { // sentinal value sent by map worker
                    mapWorkersDone++;
                    break;
                }
                std::string key(kv.key);
                if (!buffer.count(key)) {
                    buffer[key] = {};
                }
                buffer[key].push_back(kv.val);
            }
            
        }

        // process buffer when all map workers are done
        KeyValue outgoingKVs[buffer.size()];
        int sizeOf = 0;
        for (const auto&[key, value] : buffer) {
            char tempKey[8];
            strcpy(tempKey, key.c_str());
            int tempVals[value.size()];
            for (int i = 0; i < value.size(); i++) {
                tempVals[i] = value[i];
            }
            KeyValue kv = reduce(tempKey, tempVals, value.size());
            outgoingKVs[sizeOf].val = kv.val;
            strcpy(outgoingKVs[sizeOf].key, kv.key);
            sizeOf++;
        }

        // send reduced value to master
        MPI_Send(outgoingKVs, sizeOf, istruct, 0, TAG_COMM_PAIR_LIST, MPI_COMM_WORLD);


#if DEBUG_REDUCE_WORKER
        printf("Rank (%d): This is a reduce worker process\n", rank);
        printf("<>(%d): Index of reduce worker is done\n", reduceWorkerIdx);
#endif
    }

    //Clean up
    MPI_Finalize();
    return 0;
}

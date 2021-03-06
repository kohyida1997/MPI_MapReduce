#define DEBUG_WORK_DISTRIBUTION 0
#define DEBUG_MAP_WORKER 0
#define DEBUG_MASTER 0
#define DEBUG_REDUCE_WORKER 0

#define KEY_LENGTH 8 // use hardcoded value from tasks.h // char key[8];
#define TAG_MASTER_TO_MAP_SIZE 1
#define TAG_MASTER_TO_MAP_FILE 2
#define TAG_MAP_TO_REDUCE_SIZE 11
#define TAG_MAP_TO_REDUCE_DICT 12
#define TAG_REDUCE_TO_MASTER_SIZE 21
#define TAG_REDUCE_TO_MASTER_DICT 22

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <string.h>
#include <functional>
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



int main(int argc, char** argv) {
    constexpr int MASTER_RANK = 0;
    constexpr int SIZE_TAG = 0;
    constexpr int FILE_TAG = 1;
    MPI_Init(&argc, &argv);

    int world_size, rank;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    if (world_size < 3) {
        std::cout << "Need world_size to be at least 3. Use -np 3 as cmd line arg" << std::endl;
        //Clean up
        MPI_Finalize();
        return 0;
    }

    // Get command-line params
    char *input_files_dir = argv[1];
    int num_files = atoi(argv[2]);
    int num_map_workers = atoi(argv[3]);
    int num_reduce_workers = atoi(argv[4]);
    char *output_file_name = argv[5];
    int map_reduce_task_num = atoi(argv[6]);

    if ((1 + num_map_workers + num_reduce_workers) > world_size) {
        num_map_workers = world_size - 2;
        num_reduce_workers = 1;
    } else if ((1 + num_map_workers + num_reduce_workers) < world_size) {
        num_map_workers = world_size - 1 - num_reduce_workers;
    }

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

    // MPI struct to transfer KeyValue struct from one node to another node
    const int nfields = 2;
    MPI_Aint displacements[nfields];
    int blocklens[] = {KEY_LENGTH, 1};
    MPI_Datatype types[] = {MPI_CHAR, MPI_INT};

    displacements[0] = offsetof(KeyValue, key);
    displacements[1] = offsetof(KeyValue, val);

    MPI_Datatype keyValueMsg;
    MPI_Type_create_struct(nfields, blocklens, displacements, types, &keyValueMsg);
    MPI_Type_commit(&keyValueMsg);


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

#if DEBUG_MASTER
            std::cout << "Rank (MASTER) Sending File " << filePath << " to MapWorker Rank = " << workerToSendTo << std::endl;
#endif
            MPI_Send(&currFileSize, 1, MPI_INT, workerToSendTo, TAG_MASTER_TO_MAP_SIZE, MPI_COMM_WORLD); // size
            MPI_Send(currFileContents, currFileSize, MPI_CHAR, workerToSendTo, TAG_MASTER_TO_MAP_FILE, MPI_COMM_WORLD); // actual file
        }

        /* Receive from reduce workers and write to file */
        std::ofstream myfile;
        myfile.open (output_file_name);

        for (int reduceWorkerIdx = 0; reduceWorkerIdx < num_reduce_workers; reduceWorkerIdx++) {
            MPI_Status status;
            int incomingKVSize = 1;
            // Wait for size msg from MPI_ANY_SOURCE
            MPI_Recv(&incomingKVSize, 1, MPI_INT, MPI_ANY_SOURCE, TAG_REDUCE_TO_MASTER_SIZE, MPI_COMM_WORLD, &status);
            KeyValue incomingKVs[incomingKVSize]; 
            // then wait for KV array from status.MPI_SOURCE of size msg
            MPI_Recv(incomingKVs, incomingKVSize, keyValueMsg, status.MPI_SOURCE, TAG_REDUCE_TO_MASTER_DICT, MPI_COMM_WORLD, &status);
            for (int i = 0; i < incomingKVSize; i++) {
                KeyValue kv = (incomingKVs)[i];
                myfile << kv.key << " " << kv.val << std::endl;
            }
        }
        myfile.close();

        /* Free all memory allocated for files. */
        for (int i = 0; i < num_files; i++)
            free(filePtrs[i]);

#if DEBUG_MASTER
        printf("Rank (MASTER): Master is Done\n");
#endif
    } else if ((rank >= 1) && (rank <= num_map_workers)) {
        
        int mapWorkerIdx = rank - 1;
        int fileIdxToExpect = mapWorkerIdx;
        int temp = mapWorkerFilesToProcess[mapWorkerIdx];
        /* While this map worker still has stuff to process. */
        while (temp > 0) {
            int size;
            MPI_Status status;
            char* fileContent;

            MPI_Recv(&size, 1, MPI_INT, MASTER_RANK, TAG_MASTER_TO_MAP_SIZE, MPI_COMM_WORLD, &status);
            
            /* Need to free */
            fileContent = (char*) malloc(size);

            MPI_Recv(fileContent, size, MPI_CHAR, MASTER_RANK, TAG_MASTER_TO_MAP_FILE, MPI_COMM_WORLD, &status);
#if DEBUG_MAP_WORKER
            printf("Rank (%d): Map Worker successfully received file contents\n", rank);
            printf("Rank (%d): Map Worker is working on Mapping File [%d.txt] \n", rank, fileIdxToExpect);
#endif
            /* Do the actual Mapping */
            MapTaskOutput* res = map(fileContent);

            std::hash<std::string> hasher;
            for (int reduceWorkerIdx = 0; reduceWorkerIdx < num_reduce_workers; reduceWorkerIdx++) {
                KeyValue outgoingKVs[res->len];
                int sizeOf = 0;
                for (int i = 0; i < res->len; i++) {
                    KeyValue kv = (res -> kvs)[i];
                    std::string str(kv.key);
                    auto hashed = hasher(str); //returns std::size_t
                    if (hashed % num_reduce_workers != reduceWorkerIdx) {
                        continue;
                    }
                    outgoingKVs[sizeOf].val = kv.val;
                    strcpy(outgoingKVs[sizeOf].key, kv.key);
                    sizeOf++;
                }
                int reduceWorkerRank = reduceWorkerIdx + 1 + num_map_workers;
                MPI_Send(&sizeOf, 1, MPI_INT, reduceWorkerRank, TAG_MAP_TO_REDUCE_SIZE, MPI_COMM_WORLD); // size
                MPI_Send(outgoingKVs, sizeOf, keyValueMsg, reduceWorkerRank, TAG_MAP_TO_REDUCE_DICT, MPI_COMM_WORLD);
            }

#if DEBUG_MAP_WORKER
            printf("Rank (%d): Map Worker is done Mapping File [%d.txt] \n", rank, fileIdxToExpect);
#endif
            free_map_task_output(res);
            fileIdxToExpect += num_map_workers;
            temp--;
            free(fileContent);
        }

        /* 
        Send sentinal value to reduce worker to indicate that this map worker is done
        */
        for (int reduceWorkerIdx = 0; reduceWorkerIdx < num_reduce_workers; reduceWorkerIdx++) {
            int sizeOf = -1;
            int reduceWorkerRank = reduceWorkerIdx + 1 + num_map_workers;
            MPI_Send(&sizeOf, 1, MPI_INT, reduceWorkerRank, TAG_MAP_TO_REDUCE_SIZE, MPI_COMM_WORLD); // size
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
            MPI_Status status;
            int incomingKVSize = 1;
            // Wait for size msg from MPI_ANY_SOURCE
            MPI_Recv(&incomingKVSize, 1, MPI_INT, MPI_ANY_SOURCE, TAG_MAP_TO_REDUCE_SIZE, MPI_COMM_WORLD, &status);
            if (incomingKVSize == -1) {
                // sentinal value sent by map worker
                mapWorkersDone++;
                continue;
            }
            KeyValue incomingKVs[incomingKVSize]; 
            // then wait for KV array from status.MPI_SOURCE of size msg
            MPI_Recv(incomingKVs, incomingKVSize, keyValueMsg, status.MPI_SOURCE, TAG_MAP_TO_REDUCE_DICT, MPI_COMM_WORLD, &status);
            for (int i = 0; i < incomingKVSize; i++) {
                KeyValue kv = (incomingKVs)[i];
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
        for (const auto &iterator : buffer) {
            auto key = iterator.first;
            auto value = iterator.second;
            char tempKey[KEY_LENGTH];
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
        MPI_Send(&sizeOf, 1, MPI_INT, 0, TAG_REDUCE_TO_MASTER_SIZE, MPI_COMM_WORLD); // size
        MPI_Send(outgoingKVs, sizeOf, keyValueMsg, 0, TAG_REDUCE_TO_MASTER_DICT, MPI_COMM_WORLD);


#if DEBUG_REDUCE_WORKER
        printf("Rank (%d): This is a reducer worker is done\n", rank);
#endif
    }

    //Clean up
    MPI_Finalize();
    return 0;
}

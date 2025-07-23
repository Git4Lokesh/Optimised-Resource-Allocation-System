#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <sys/ipc.h>
#include <sys/msg.h>
#include <sys/shm.h>
#include <unistd.h>
#include <time.h>
#include <sys/types.h>
#include <stdbool.h>

#define MAX_DOCKS 30
#define MAX_DOCK_CATEGORY 25
#define MAX_SHIP_CATEGORY 25
#define MAX_REG_INCOMING_SHIPS 500
#define MAX_EME_INCOMING_SHIPS 100
#define MAX_OUTGOING_SHIPS 500
#define MAX_CRANE_CAP 30
#define MAX_SOLVERS 8
#define MIN_SOLVERS 2
#define MAX_CARGO_COUNT 200
#define MAX_AUTH_STRING_LEN 100
#define MAX_NEW_REQUESTS 100
#define SHIP_WAITING 0
#define SHIP_DOCKED 1
#define SHIP_PROCESSING 2
#define SHIP_COMPLETED 3
#define SHIP_LEFT 4

typedef struct MessageStruct {
    long mtype;
    int timestep;
    int shipId;
    int direction;
    int dockId;
    int cargoId;
    int isFinished;
    union {
        int numShipRequests;
        int craneId;
    };
} MessageStruct;

typedef struct ShipRequest{
    int shipId;
    int timestep;
    int category;
    int direction;
    int emergency;
    int waitingTime;
    int numCargo;
    int cargo[MAX_CARGO_COUNT];
} ShipRequest;

typedef struct MainSharedMemory{
    char authStrings[MAX_DOCKS][MAX_AUTH_STRING_LEN];
    ShipRequest newShipRequests[MAX_NEW_REQUESTS];
} MainSharedMemory;

typedef struct SolverRequest{
    long mtype;
    int dockId;
    char authStringGuess[MAX_AUTH_STRING_LEN];
} SolverRequest;

typedef struct SolverResponse{
    long mtype;
    int guessIsCorrect;
} SolverResponse;

typedef struct Dock{
    int category;
    int *crane_weights;
    int is_occupied;
    int current_ship_id;
    int* crane_status; // 0: available, 1: busy
    int last_undocked_timestep;
} Dock;

typedef struct ShipTracking{
    ShipRequest ship;
    int status; // 0: waiting, 1: docked, 2: processing, 3: completed, 4: left
    int assigned_dock;
    int* cargo_status; // 0: pending, 1: processed
    int docking_timestep; // When the ship was docked
    int last_cargo_processed_timestep; // When the last cargo was processed
    int undock_attempts; // Number of attempts to guess the frequency
} ShipTracking;

// Global variables
ShipTracking ship_req_array[10000];
int ship_count = 0;

// Function to free resources for a ship
void free_ship_resources(ShipTracking *ship) {
    if (ship->cargo_status != NULL) {
        free(ship->cargo_status);
        ship->cargo_status = NULL;
    }
}

// Function to release cranes after cargo processing
void release_cranes(Dock *dock) {
    for (int i = 0; i < dock->category; i++) {
        dock->crane_status[i] = 0; // Mark all cranes as available
    }
}

// Helper function to send a message
int send_message(int msgid, MessageStruct *msg) {
    if (msgsnd(msgid, msg, sizeof(MessageStruct) - sizeof(long), 0) == -1) {
        perror("Error sending message");
        return 0;
    }
    return 1;
}

// Add these functions above the main function
void send_dock_message(int msgid, int timestep, int shipId, int direction, int dockId) {
    MessageStruct msg;
    memset(&msg, 0, sizeof(MessageStruct));
    msg.mtype = 2; // Docking message type
    msg.timestep = timestep;
    msg.shipId = shipId;
    msg.direction = direction;
    msg.dockId = dockId;
    
    send_message(msgid, &msg);
}

void send_cargo_message(int msgid, int timestep, int shipId, int dockId, int cargoId, int craneId, int direction) {
    MessageStruct msg;
    memset(&msg, 0, sizeof(MessageStruct));
    msg.mtype = 4; // Cargo processing message type
    msg.timestep = timestep;
    msg.shipId = shipId;
    msg.dockId = dockId;
    msg.cargoId = cargoId;
    msg.craneId = craneId;
    msg.direction = direction;
    
    send_message(msgid, &msg);
}

int try_radio_frequency_auth(int main_msgid, int *solver_msgids, int num_solvers, 
    MainSharedMemory *main_shm, int dock_id, int ship_id, 
    int current_timestep, int docking_timestep, int last_cargo_timestep,
    int ship_direction, bool *auth_string_known) { 
    
    // Try each solver in sequence rather than randomly
    static int next_solver = 0;
    int solver_idx = next_solver;
    next_solver = (next_solver + 1) % num_solvers;
    
    // Step 1: Inform solver which dock we're guessing for
    SolverRequest req1;
    memset(&req1, 0, sizeof(SolverRequest));
    req1.mtype = 1; // First step message type
    req1.dockId = dock_id;
    
    if (msgsnd(solver_msgids[solver_idx], &req1, sizeof(SolverRequest) - sizeof(long), IPC_NOWAIT) == -1) {
        if (errno == EAGAIN) {
            // Queue full, try another solver
            solver_idx = (solver_idx + 1) % num_solvers;
            if (msgsnd(solver_msgids[solver_idx], &req1, sizeof(SolverRequest) - sizeof(long), 0) == -1) {
                perror("Error sending dock selection to solver (retry)");
                return 0;
            }
        } else {
            perror("Error sending dock selection to solver");
            return 0;
        }
    }
    
    
    // Calculate string length based on timestamps with validation
    int auth_string_len = last_cargo_timestep - docking_timestep;
    
    if (auth_string_len <= 0) {
        printf("Invalid string length calculation: %d (last_cargo=%d, docking=%d)\n", 
               auth_string_len, last_cargo_timestep, docking_timestep);
        return 0;
    }
    
    // Limit string length to prevent buffer overflows
    if (auth_string_len >= MAX_AUTH_STRING_LEN) {
        auth_string_len = MAX_AUTH_STRING_LEN - 1;
    }
    
    // Track the current guess string so we can store it if successful
    char current_guess[MAX_AUTH_STRING_LEN];
    
    // IMPROVED: For all docks, track consecutive failures to generate fresh guesses
    static int *consecutive_failures = NULL;
    if (consecutive_failures == NULL) {
        consecutive_failures = calloc(MAX_DOCKS, sizeof(int));
    }
    
    // After even a single failure, try fresh guesses (strict policy for all docks)
    if (consecutive_failures[dock_id] > 0) {
        // Reset the counter and mark string as unknown to force new guesses
        consecutive_failures[dock_id] = 0;
        auth_string_known[dock_id] = false;
        memset(main_shm->authStrings[dock_id], 0, MAX_AUTH_STRING_LEN);
    }
    
    // Track which attempt we're on for this dock
    static int *dock_attempts = NULL;
    if (dock_attempts == NULL) {
        dock_attempts = calloc(MAX_DOCKS, sizeof(int));
        if (dock_attempts == NULL) {
            perror("Failed to allocate memory for dock attempts");
            return 0;
        }
    }
    
    // Define the possible characters (5,6,7,8,9,.)
    char arr[6] = {'5', '6', '7', '8', '9', '.'};
    
    // Calculate max possible combinations
    int maxn = 5 * 5; // First and last digit (5 possibilities each)
    for (int i = 0; i < auth_string_len - 2; i++)
        maxn *= 6;  // Middle positions (6 possibilities each)
        
    // Get current attempt number and increment for next time
    int attempt = dock_attempts[dock_id]++;
    
    // If we've tried all possibilities, start over (though this shouldn't happen)
    if (attempt >= maxn) {
        attempt = 0;
        dock_attempts[dock_id] = 1;
    }
    
    // Create the request
    SolverRequest req2;
    memset(&req2, 0, sizeof(SolverRequest));
    req2.mtype = 2; // Guess message type
    req2.dockId = dock_id;
    
    // Generate a general pattern that works well for all docks
    // For all docks, use a systematic but varying pattern based on attempt
    int temp = attempt;
    
    // Always set first and last characters
    req2.authStringGuess[0] = arr[temp % 5];  // First char (5-9)
    temp /= 5;
    req2.authStringGuess[auth_string_len - 1] = arr[temp % 5];  // Last char (5-9)
    temp /= 5;
    
    // Generate middle characters
    for (int j = 1; j < auth_string_len - 1; j++) {
        req2.authStringGuess[j] = arr[temp % 6];  // Middle chars (5-9 or .)
        temp /= 6;
    }
    req2.authStringGuess[auth_string_len] = '\0';  // Null terminate
    
    // Save our current guess
    strncpy(current_guess, req2.authStringGuess, MAX_AUTH_STRING_LEN);
    
    if (msgsnd(solver_msgids[solver_idx], &req2, sizeof(SolverRequest) - sizeof(long), 0) == -1) {
        perror("Error sending guess to solver");
        return 0;
    }
    
    // Wait for response
    SolverResponse resp;
    if (msgrcv(solver_msgids[solver_idx], &resp, sizeof(SolverResponse) - sizeof(long), 3, 0) == -1) {
        perror("Error receiving response from solver");
        return 0;
    }
    
    // Check response and handle accordingly
    if (resp.guessIsCorrect == 1) {
        // Guess was correct - store the string in shared memory and mark as verified
        strncpy(main_shm->authStrings[dock_id], current_guess, MAX_AUTH_STRING_LEN);
        auth_string_known[dock_id] = true;
        consecutive_failures[dock_id] = 0; // Reset failure counter on success
        
        // Send undock message to validation module
        MessageStruct undock_msg;
        memset(&undock_msg, 0, sizeof(MessageStruct));
        undock_msg.mtype = 3; // Undocking message type
        undock_msg.timestep = current_timestep;
        undock_msg.shipId = ship_id;
        undock_msg.dockId = dock_id;
        undock_msg.isFinished = 1; // Ship is leaving successfully
        undock_msg.direction = (ship_direction == 1) ? 1 : -1;
        
        if (msgsnd(main_msgid, &undock_msg, sizeof(MessageStruct) - sizeof(long), 0) == -1) {
            perror("Error sending undock message");
            return 0;
        }
        
        return 1;
    } else if (resp.guessIsCorrect == -1) {
        return 0;
    } else {
        // If failed, increment failure counter
        consecutive_failures[dock_id]++; // Track failures
        return 0;
    }
}

int main(int argc, char *argv[]) {
    if (argc != 2) {
        fprintf(stderr, "Usage: %s <testcase_number>\n", argv[0]);
        return 1;
    }
    
    char filename[50];
    snprintf(filename, sizeof(filename), "testcase%s/input.txt", argv[1]);
    FILE *file = fopen(filename, "r");
    if (!file) {
        perror("Error opening file");
        return 1;
    }
    
    // Read shared memory key, main message queue key, and number of solvers
    int shared_memory_key, main_message_queue_key, m;
    fscanf(file, "%d %d %d", &shared_memory_key, &main_message_queue_key, &m);
    
    // Read solver message queue keys
    int *solver_queue_keys = malloc(m * sizeof(int));
    if (!solver_queue_keys) {
        fprintf(stderr, "Memory allocation failed\n");
        fclose(file);
        return 1;
    }
    
    for (int i = 0; i < m; i++) {
        fscanf(file, "%d", &solver_queue_keys[i]);
    }
    
    // Read number of docks
    int n;
    fscanf(file, "%d", &n);
    
    // Allocate memory for dock array
    Dock *docks = malloc(n * sizeof(Dock));
    if (!docks) {
        fprintf(stderr, "Memory allocation failed for docks\n");
        free(solver_queue_keys);
        fclose(file);
        return 1;
    }
    
    // Read dock information
    for (int i = 0; i < n; i++) {
        fscanf(file, "%d", &docks[i].category);
        
        // Allocate memory for crane weights and status
        docks[i].crane_weights = malloc(docks[i].category * sizeof(int));
        docks[i].crane_status = malloc(docks[i].category * sizeof(int));
        
        if (!docks[i].crane_weights || !docks[i].crane_status) {
            fprintf(stderr, "Memory allocation failed for crane data\n");
            // Clean up resources
            for (int j = 0; j <= i; j++) {
                free(docks[j].crane_weights);
                free(docks[j].crane_status);
            }
            free(docks);
            free(solver_queue_keys);
            fclose(file);
            return 1;
        }
        
        // Initialize dock structure
        docks[i].is_occupied = 0;
        docks[i].current_ship_id = -1;
        docks[i].last_undocked_timestep = -1;
        
        // Initialize crane status and read weights
        for (int j = 0; j < docks[i].category; j++) {
            docks[i].crane_status[j] = 0; // Mark as available
            fscanf(file, "%d", &docks[i].crane_weights[j]);
        }
    }
    
    fclose(file);
    
    // Connect to main message queue
    int main_msgid = msgget(main_message_queue_key, 0666|IPC_CREAT);
    if (main_msgid == -1) {
        perror("Error accessing main message queue");
        free(solver_queue_keys);
        for (int i = 0; i < n; i++) {
            free(docks[i].crane_weights);
            free(docks[i].crane_status);
        }
        free(docks);
        return 1;
    }
    
    // Connect to solver message queues
    int *solver_msgids = malloc(m * sizeof(int));
    if (!solver_msgids) {
        perror("Memory allocation failed for solver message queue IDs");
        free(solver_queue_keys);
        for (int i = 0; i < n; i++) {
            free(docks[i].crane_weights);
            free(docks[i].crane_status);
        }
        free(docks);
        return 1;
    }
    
    for (int i = 0; i < m; i++) {
        solver_msgids[i] = msgget(solver_queue_keys[i], 0666|IPC_CREAT);
        if (solver_msgids[i] == -1) {
            fprintf(stderr, "Error accessing solver message queue %d: %s\n", i, strerror(errno));
            free(solver_queue_keys);
            free(solver_msgids);
            for (int j = 0; j < n; j++) {
                free(docks[j].crane_weights);
                free(docks[j].crane_status);
            }
            free(docks);
            return 1;
        }
    }
    
    // Connect to shared memory segment
    int shmid = shmget(shared_memory_key, sizeof(MainSharedMemory), 0666|IPC_CREAT);
    if (shmid == -1) {
        perror("Error accessing shared memory segment");
        free(solver_queue_keys);
        free(solver_msgids);
        for (int i = 0; i < n; i++) {
            free(docks[i].crane_weights);
            free(docks[i].crane_status);
        }
        free(docks);
        return 1;
    }
    
    // Attach to the shared memory segment
    MainSharedMemory *main_shm = (MainSharedMemory *)shmat(shmid, NULL, 0);
    if (main_shm == (MainSharedMemory *)-1) {
        perror("Error attaching to shared memory segment");
        free(solver_queue_keys);
        free(solver_msgids);
        for (int i = 0; i < n; i++) {
            free(docks[i].crane_weights);
            free(docks[i].crane_status);
        }
        free(docks);
        return 1;
    }
    
    // Initialize tracking for verified auth strings
    bool *auth_string_known = calloc(MAX_DOCKS, sizeof(bool));
    if (!auth_string_known) {
        perror("Failed to allocate memory for auth string tracking");
        free(solver_queue_keys);
        free(solver_msgids);
        for (int i = 0; i < n; i++) {
            free(docks[i].crane_weights);
            free(docks[i].crane_status);
        }
        free(docks);
        shmdt(main_shm);
        return 1;
    }
    
    int current_timestep = 1;
    
    while (1) {
        // Reset all crane status at the beginning of each timestep (except the first one)
        if (current_timestep > 1) {
            for (int d = 0; d < n; d++) {
                if (docks[d].is_occupied) {
                    for (int c = 0; c < docks[d].category; c++) {
                        docks[d].crane_status[c] = 0; // Mark all cranes as available
                    }
                }
            }
        }
        
        // Receive messages from validation module
        MessageStruct msg;
        
        if (msgrcv(main_msgid, &msg, sizeof(MessageStruct) - sizeof(long), 1, IPC_NOWAIT) != -1) {
            // Check if the test case is complete
            if (msg.isFinished == 1) {
                printf("Test case complete at timestep %d. Exiting...\n", current_timestep);
                break;
            }
            
            if (msg.timestep == current_timestep) {
                // Process new ship requests for this timestep
                printf("Timestep %d: Received %d new ship requests\n",
                       current_timestep, msg.numShipRequests);
                
                // Read new ship requests from shared memory
                for (int i = 0; i < msg.numShipRequests; i++) {
                    ShipRequest new_ship = main_shm->newShipRequests[i];
                    
                    // Add to tracking
                    ship_req_array[ship_count].ship = new_ship;
                    ship_req_array[ship_count].status = SHIP_WAITING;
                    ship_req_array[ship_count].assigned_dock = -1;
                    ship_req_array[ship_count].cargo_status = malloc(new_ship.numCargo * sizeof(int));
                    if (!ship_req_array[ship_count].cargo_status) {
                        fprintf(stderr, "Memory allocation failed for cargo status\n");
                        continue;
                    }
                    
                    ship_req_array[ship_count].docking_timestep = -1;
                    ship_req_array[ship_count].last_cargo_processed_timestep = -1;
                    ship_req_array[ship_count].undock_attempts = 0;
                    
                    memset(ship_req_array[ship_count].cargo_status, 0, new_ship.numCargo * sizeof(int));
                    
                    ship_count++;
                }
            }
        } else if (errno != ENOMSG) {
            printf("Error receiving message: %s\n", strerror(errno));
            break;
        }
        
        // Process ships for docking (emergency ships first)
        // Count available docks and waiting emergency ships
        int free_docks = 0;
        int waiting_emergency_ships = 0;
        
        for (int d = 0; d < n; d++) {
            if (!docks[d].is_occupied && docks[d].last_undocked_timestep != current_timestep) {
                free_docks++;
            }
        }
        
        for (int i = 0; i < ship_count; i++) {
            if (ship_req_array[i].status == SHIP_WAITING &&
                ship_req_array[i].ship.emergency == 1 &&
                ship_req_array[i].ship.direction == 1) {
                waiting_emergency_ships++;
            }
        }
        
        if (waiting_emergency_ships > 0 && free_docks > 0) {
            // Store original dock occupation states
            int *original_dock_states = malloc(n * sizeof(int));
            int *can_assign = calloc(waiting_emergency_ships, sizeof(int));
            ShipTracking *emergency_ships = malloc(waiting_emergency_ships * sizeof(ShipTracking));
            int *emergency_ship_indices = malloc(waiting_emergency_ships * sizeof(int));
            
            if (!original_dock_states || !can_assign || !emergency_ships || !emergency_ship_indices) {
                free(original_dock_states);
                free(can_assign);
                free(emergency_ships);
                free(emergency_ship_indices);
                continue;
            }
            
            for (int d = 0; d < n; d++) {
                original_dock_states[d] = docks[d].is_occupied;
            }
            
            // Collect all emergency ships and their indices
            int emergency_idx = 0;
            for (int i = 0; i < ship_count; i++) {
                if (ship_req_array[i].status == SHIP_WAITING &&
                    ship_req_array[i].ship.emergency == 1 &&
                    ship_req_array[i].ship.direction == 1) {
                    emergency_ships[emergency_idx] = ship_req_array[i];
                    emergency_ship_indices[emergency_idx] = i;
                    emergency_idx++;
                }
            }
            
            // Sort by multiple criteria for better scheduling
            for (int i = 0; i < emergency_idx - 1; i++) 
            {
                for (int j = i + 1; j < emergency_idx; j++) 
                {
                    if (emergency_ships[i].ship.category > emergency_ships[j].ship.category ||
                    (emergency_ships[i].ship.category == emergency_ships[j].ship.category && 
                    emergency_ships[i].ship.numCargo < emergency_ships[j].ship.numCargo)) 
                    {
                        // Prioritize lower category and higher cargo count
                        ShipTracking temp_ship = emergency_ships[i];
                        emergency_ships[i] = emergency_ships[j];
                        emergency_ships[j] = temp_ship;
                        int temp_idx = emergency_ship_indices[i];
                        emergency_ship_indices[i] = emergency_ship_indices[j];
                        emergency_ship_indices[j] = temp_idx;
                    }
                }
            }

            
            // Find maximum possible assignments using greedy approach
            int *dock_assignments = malloc(emergency_idx * sizeof(int));
            if (!dock_assignments) {
                free(original_dock_states);
                free(can_assign);
                free(emergency_ships);
                free(emergency_ship_indices);
                continue;
            }
            
            // Reset dock states for simulation
            for (int d = 0; d < n; d++) {
                docks[d].is_occupied = original_dock_states[d];
            }
            
            // For each ship, find the lowest category dock that can accommodate it
            for (int i = 0; i < emergency_idx; i++) {
                int best_dock = -1;
                int best_category = MAX_DOCK_CATEGORY + 1;
                
                for (int d = 0; d < n; d++) {
                    if (!docks[d].is_occupied &&
                        docks[d].last_undocked_timestep != current_timestep &&
                        docks[d].category >= emergency_ships[i].ship.category &&
                        docks[d].category < best_category) {
                        best_dock = d;
                        best_category = docks[d].category;
                    }
                }
                
                if (best_dock != -1) {
                    can_assign[i] = 1;
                    dock_assignments[i] = best_dock;
                    docks[best_dock].is_occupied = 1;
                } else {
                    dock_assignments[i] = -1;
                }
            }
            
            // Reset dock states for ACTUAL assignments
            for (int d = 0; d < n; d++) {
                docks[d].is_occupied = original_dock_states[d];
            }
            
            // Make actual assignments and send messages
            for (int i = 0; i < emergency_idx; i++) {
                if (can_assign[i] && dock_assignments[i] != -1) {
                    int ship_idx = emergency_ship_indices[i];
                    int dock_id = dock_assignments[i];
                    
                    if (!docks[dock_id].is_occupied && docks[dock_id].last_undocked_timestep != current_timestep) {
                        // Update ship and dock status
                        docks[dock_id].is_occupied = 1;
                        docks[dock_id].current_ship_id = ship_req_array[ship_idx].ship.shipId;
                        ship_req_array[ship_idx].assigned_dock = dock_id;
                        ship_req_array[ship_idx].status = SHIP_DOCKED;
                        ship_req_array[ship_idx].docking_timestep = current_timestep;
                        
                        // Send docking message
                        send_dock_message(main_msgid, current_timestep, ship_req_array[ship_idx].ship.shipId, 
                                          1, dock_id);
                    }
                }
            }
            
            free(emergency_ships);
            free(can_assign);
            free(emergency_ship_indices);
            free(dock_assignments);
            free(original_dock_states);
        }
        
        // Handle regular incoming and outgoing ships
        for (int i = 0; i < ship_count; i++) {
            // Skip if not waiting or if it's an emergency ship (already handled)
            if (ship_req_array[i].status != SHIP_WAITING ||
                (ship_req_array[i].ship.direction == 1 && ship_req_array[i].ship.emergency == 1)) {
                continue;
            }
            
            // Process waiting time for regular incoming ships
            if (ship_req_array[i].ship.direction == 1 && ship_req_array[i].ship.emergency == 0) {
                // Calculate when this ship's waiting period ends
                int wait_end_timestep = ship_req_array[i].ship.timestep + ship_req_array[i].ship.waitingTime;
                
                // If we've exceeded the waiting time, mark ship to leave
                if (current_timestep > wait_end_timestep) {
                    // Free its resources
                    free(ship_req_array[i].cargo_status);
                    ship_req_array[i].cargo_status = NULL;
                    
                    // Remove ship by replacing with last ship in array
                    if (i < ship_count - 1) {
                        ship_req_array[i] = ship_req_array[ship_count - 1];
                        i--; // Check this position again
                    }
                    
                    ship_count--;
                    continue; // Skip to next ship
                }
            }
            
            // Try to dock the ship
            for (int d = 0; d < n; d++) {
                // Skip if dock is occupied or was just undocked in the current timestep
                if (docks[d].is_occupied || docks[d].last_undocked_timestep == current_timestep) {
                    continue;
                }
                
                // Check if dock is available and category compatible
                if (docks[d].category >= ship_req_array[i].ship.category) {
                    // Assign ship to dock
                    docks[d].is_occupied = 1;
                    docks[d].current_ship_id = ship_req_array[i].ship.shipId;
                    ship_req_array[i].assigned_dock = d;
                    ship_req_array[i].status = SHIP_DOCKED;
                    ship_req_array[i].docking_timestep = current_timestep;
                    
                    // Determine direction (1 for incoming, -1 for outgoing)
                    int direction = (ship_req_array[i].ship.direction == 1) ? 1 : -1;
                    
                    // Send docking message
                    send_dock_message(main_msgid, current_timestep, ship_req_array[i].ship.shipId, 
                                      direction, d);
                    
                    break; // Found a dock for this ship
                }
            }
        }
        
        // Process cargo loading/unloading for docked ships
        for (int i = 0; i < ship_count; i++) {
            // Skip if not docked or processing, or just docked in current timestep
            if ((ship_req_array[i].status != SHIP_DOCKED && ship_req_array[i].status != SHIP_PROCESSING) || 
                ship_req_array[i].docking_timestep == current_timestep) {
                continue;
            }
            
            int dock_id = ship_req_array[i].assigned_dock;
            ShipRequest *ship = &ship_req_array[i].ship;
            
            // Find all pending cargo
            int pending_cargo[MAX_CARGO_COUNT];
            int pending_count = 0;
            
            for (int cargo_idx = 0; cargo_idx < ship->numCargo; cargo_idx++) {
                if (ship_req_array[i].cargo_status[cargo_idx] == 0) {
                    pending_cargo[pending_count++] = cargo_idx;
                }
            }
            
            // Assign cranes to process cargo with capacity check
            for (int j = 0; j < pending_count; j++) {
                int cargo_idx = pending_cargo[j];
                int cargo_weight = ship->cargo[cargo_idx];
                
                // Find an available crane with sufficient capacity
                for (int c = 0; c < docks[dock_id].category; c++) {
                    if (docks[dock_id].crane_status[c] == 0 && docks[dock_id].crane_weights[c] >= cargo_weight) {
                        // Process this cargo with this crane
                        ship_req_array[i].cargo_status[cargo_idx] = 1;
                        docks[dock_id].crane_status[c] = 1; // Mark crane as busy
                        
                        // Record the time this cargo was processed
                        ship_req_array[i].last_cargo_processed_timestep = current_timestep;
                        ship_req_array[i].status = SHIP_PROCESSING;
                        
                        // Determine direction (1 for incoming, -1 for outgoing)
                        int direction = (ship->direction == 1) ? 1 : -1;
                        
                        // Send cargo processing message
                        send_cargo_message(main_msgid, current_timestep, ship->shipId, dock_id, 
                                           cargo_idx, c, direction);
                        
                        break; // Found a crane for this cargo
                    }
                }
            }
            
            // Check if all cargo is processed
            int all_processed = 1;
            for (int cargo_idx = 0; cargo_idx < ship->numCargo; cargo_idx++) {
                if (ship_req_array[i].cargo_status[cargo_idx] == 0) {
                    all_processed = 0;
                    break;
                }
            }
            
            if (all_processed) {
                // Check if we're in the timestep after the last cargo was processed
                if (current_timestep >= ship_req_array[i].last_cargo_processed_timestep + 1) {
                    // Try authentication
                    if (ship_req_array[i].status != SHIP_COMPLETED) {
                        ship_req_array[i].status = SHIP_COMPLETED;
                    }
                    
                    int auth_success = try_radio_frequency_auth(
                        main_msgid, solver_msgids, m, main_shm, 
                        dock_id, ship->shipId, current_timestep,
                        ship_req_array[i].docking_timestep,
                        ship_req_array[i].last_cargo_processed_timestep,
                        ship->direction,
                        auth_string_known);
                    
                    ship_req_array[i].undock_attempts++;
                    
                    if (auth_success) {
                        // Authentication successful, ship can undock
                        ship_req_array[i].status = SHIP_LEFT;
                        docks[dock_id].is_occupied = 0;
                        docks[dock_id].current_ship_id = -1;
                        docks[dock_id].last_undocked_timestep = current_timestep;
                        
                        // Free ship resources
                        free_ship_resources(&ship_req_array[i]);
                    }
                }
            }
        }
        
        // Clean up ships that have left
        for (int i = 0; i < ship_count; i++) {
            if (ship_req_array[i].status == SHIP_LEFT) {
                // Remove ship by replacing with last ship in array
                if (i < ship_count - 1) {
                    ship_req_array[i] = ship_req_array[ship_count - 1];
                    i--; // Check this position again
                }
                ship_count--;
            }
        }
        
        // Handle ships needing authentication
        int ships_needing_auth = 0;
        for (int i = 0; i < ship_count; i++) {
            if (ship_req_array[i].status == SHIP_COMPLETED) {
                ships_needing_auth++;
            }
        }
        
        if (ships_needing_auth > 0) {
            int continue_trying = 1;
            int max_attempts_per_ship = 250000;
            // Adaptively adjust attempts based on dock usage
            if (ships_needing_auth > n/2) 
            {
                max_attempts_per_ship = 300000; // Even more attempts when many ships waiting
            }
            // Check for endgame state (only authentication remains)
bool endgame_state = true;
int waiting_ships = 0;
int active_ships = 0;
int ships_needing_auth = 0;

// Count ships in different states
for (int i = 0; i < ship_count; i++) {
    if (ship_req_array[i].status == SHIP_WAITING) {
        waiting_ships++;
        endgame_state = false;
    } else if (ship_req_array[i].status == SHIP_DOCKED || 
              ship_req_array[i].status == SHIP_PROCESSING) {
        active_ships++;
        endgame_state = false;
    } else if (ship_req_array[i].status == SHIP_COMPLETED) {
        ships_needing_auth++;
    }
}

// If only authentication left, maximize auth attempts
if (endgame_state && ships_needing_auth > 0) {
    // Exponentially increase authentication attempts
    int max_attempts_per_ship = 1000000; // Much higher number
    
    // Distribute solver resources optimally
    int solvers_per_ship = (m > ships_needing_auth) ? 
                           (m / ships_needing_auth) : 1;
    
    // Skip sleep time between auth attempts in endgame
    usleep(0);
}

            while (continue_trying && ships_needing_auth > 0) {
                continue_trying = 0;
                
                for (int i = 0; i < ship_count; i++) {
                    if (ship_req_array[i].status == SHIP_COMPLETED) {
                        int dock_id = ship_req_array[i].assigned_dock;
                        ShipRequest *ship = &ship_req_array[i].ship;
                        
                        docks[dock_id].is_occupied = 1;
                        docks[dock_id].current_ship_id = ship->shipId;
                        
                        // Try authentication using known string first
                        if (auth_string_known[dock_id]) {
                            int auth_success = try_radio_frequency_auth(
                                main_msgid, solver_msgids, m, main_shm, 
                                dock_id, ship->shipId, current_timestep,
                                ship_req_array[i].docking_timestep,
                                ship_req_array[i].last_cargo_processed_timestep,
                                ship->direction,
                                auth_string_known);
                                
                            ship_req_array[i].undock_attempts++;
                            
                            if (auth_success) {
                                ship_req_array[i].status = SHIP_LEFT;
                                docks[dock_id].is_occupied = 0;
                                docks[dock_id].current_ship_id = -1;
                                docks[dock_id].last_undocked_timestep = current_timestep;
                                free_ship_resources(&ship_req_array[i]);
                                continue;
                            }
                        }
                        // Fast path: If we already know the string, skip the attempt loop
                        if (auth_string_known[dock_id]) 
                        {
                            continue_trying = 0; // Skip to next ship
                            break;
                        }
                        // Make multiple auth attempts
                        for (int attempt = 0; attempt < max_attempts_per_ship; attempt++) {
                            if (ship_req_array[i].status != SHIP_COMPLETED) {
                                break;
                            }
                            
                            int auth_success = try_radio_frequency_auth(
                                main_msgid, solver_msgids, m, main_shm, 
                                dock_id, ship->shipId, current_timestep,
                                ship_req_array[i].docking_timestep,
                                ship_req_array[i].last_cargo_processed_timestep,
                                ship->direction,
                                auth_string_known);
                            
                            ship_req_array[i].undock_attempts++;
                            
                            if (auth_success) {
                                ship_req_array[i].status = SHIP_LEFT;
                                docks[dock_id].is_occupied = 0;
                                docks[dock_id].current_ship_id = -1;
                                docks[dock_id].last_undocked_timestep = current_timestep;
                                free_ship_resources(&ship_req_array[i]);
                                break;
                            }
                        }
                        
                        if (ship_req_array[i].status == SHIP_COMPLETED) {
                            continue_trying = 1;
                        }
                    }
                }
                
                // Update ship count and remaining authentication needs
                ships_needing_auth = 0;
                for (int i = 0; i < ship_count; i++) {
                    if (ship_req_array[i].status == SHIP_LEFT) {
                        if (i < ship_count - 1) {
                            ship_req_array[i] = ship_req_array[ship_count - 1];
                            i--;
                        }
                        ship_count--;
                    } else if (ship_req_array[i].status == SHIP_COMPLETED) {
                        ships_needing_auth++;
                    }
                }
            }
        }

        
        
        // Signal validation module to advance to the next timestep
        MessageStruct timestep_msg;
        memset(&timestep_msg, 0, sizeof(MessageStruct));
        timestep_msg.mtype = 5;
        timestep_msg.timestep = current_timestep;
        
        if (msgsnd(main_msgid, &timestep_msg, sizeof(MessageStruct) - sizeof(long), 0) == -1) {
            if (errno == EINVAL) {
                // Try to reconnect to message queue
                main_msgid = msgget(main_message_queue_key, 0666|IPC_CREAT);
                if (main_msgid == -1) {
                    perror("Failed to reconnect to message queue");
                    break;
                }
                
                if (msgsnd(main_msgid, &timestep_msg, sizeof(MessageStruct) - sizeof(long), 0) == -1) {
                    perror("Still failed to send message after reconnecting");
                    break;
                }
            } else {
                usleep(5000);
                if (msgsnd(main_msgid, &timestep_msg, sizeof(MessageStruct) - sizeof(long), 0) == -1) {
                    perror("Failed second attempt to send message");
                    break;
                }
            }
        }

        // Check if we can skip timesteps when port is inactive
bool can_skip_timesteps = true;
int active_ships = 0;

for (int i = 0; i < ship_count; i++) {
    if (ship_req_array[i].status == SHIP_PROCESSING ||
        ship_req_array[i].status == SHIP_COMPLETED) {
        can_skip_timesteps = false;
        active_ships++;
        break;
    }
}

// If minimal activity, reduce sleep time
if (active_ships < n/4) {
    usleep(10000); // Shorter 10ms delay when fewer ships active
} else {
    usleep(30000); // 30ms delay otherwise
}

current_timestep++;

    }
    return 0;
}

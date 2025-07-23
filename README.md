# MIT 6.584 Lab 1: MapReduce Implementation

## Architecture Overview

```mermaid
graph TD
    A[Coordinator] -->|Assign Task| B[Worker]
    B -->|Read Input| C[Input Files]
    B -->|Write Temp| D[Intermediate Files]
    B -->|Rename| E[Output Files]
    A -->|Heartbeat| B
    B -->|Request Task| A
```

## Core Components
1. Coordinator (Master)

    Manages entire job lifecycle

    Tracks task states (Idle/In-Progress/Completed)

    Handles worker failures via timeout

    Maintains intermediate file locations

    Implements task scheduling logic

2. Worker Processes

    Request tasks from Coordinator via RPC

    Execute either Map or Reduce functions

    Store intermediate output in temporary files

    Atomically rename files upon completion

    Report results back to Coordinator

<img width="685" height="457" alt="image" src="https://github.com/user-attachments/assets/8fecb112-2f69-4c9e-a58a-f92c7edb509b" />

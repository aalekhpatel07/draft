# draft-core
This module contains the core data types used by Raft and the core RPC implementations.

TODO:
- [x] Implement RequestVoteRPC.
- [x] Write tests for RequestVoteRPC (Coverage >= 90%).
- [x] Implement AppendEntriesRPC.
- [x] Have [Storage](./src/storage.rs) as a configurable backend.
- [x] Comment the implementations heavily for improved readability and a better debugging experience.
- [x] Write tests for AppendEntriesRPC (Coverage >= 90%)
- [x] Integrate a State Machine (for example, tokio's mini-redis).
- [ ] Add more doc comments to and make a nice docs page.
- [ ] Make an book to document individual parts in a nicer way and have a reference guide.

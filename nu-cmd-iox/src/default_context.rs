use nu_protocol::engine::{EngineState, StateWorkingSet};

use crate::*;

pub fn add_iox_context(mut engine_state: EngineState) -> EngineState {
    let delta = {
        let mut working_set = StateWorkingSet::new(&engine_state);

        macro_rules! bind_command {
            ( $( $command:expr ),* $(,)? ) => {
                $( working_set.add_decl(Box::new($command)); )*
            };
        }

        // If there are commands that have the same name as default declarations,
        // they have to be registered before the main declarations. This helps to make
        // them only accessible if the correct input value category is used with the
        // declaration

        // Iox
        bind_command! {
            Ioxnamespace,
            Ioxsql,
            Ioxwrite,
            Ioxwritefile,
        }

        working_set.render()
    };

    if let Err(err) = engine_state.merge_delta(delta) {
        eprintln!("Error creating default context: {err:?}");
    }

    // Cache the table decl id so we don't have to look it up later
    let table_decl_id = engine_state.find_decl("table".as_bytes(), &[]);
    engine_state.table_decl_id = table_decl_id;

    engine_state
}

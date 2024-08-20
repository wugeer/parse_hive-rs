#![allow(clippy::redundant_clone)]

use std::{path::PathBuf, sync::Arc};

use tauri::async_runtime::Mutex;
use tauri::State;

struct AppState {
    input_text: Arc<Mutex<String>>,
    file_path: Arc<Mutex<Option<PathBuf>>>,
    result_text: Arc<Mutex<String>>,
}

fn main() {
    tauri::Builder::default()
        .manage(AppState {
            input_text: Arc::new(Mutex::new(String::new())),
            file_path: Arc::new(Mutex::new(None)),
            result_text: Arc::new(Mutex::new(String::new())),
        })
        .invoke_handler(tauri::generate_handler![
            set_input_text,
            set_file_path,
            calculate_md5,
            get_result_text
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

#[tauri::command]
async fn set_input_text(text: String, state: State<'_, AppState>) {
    *state.input_text.lock().await = text;
}

#[tauri::command]
async fn set_file_path(path: PathBuf, state: State<'_, AppState>) {
    *state.file_path.lock().await = Some(path);
}

// #[tauri::command]
// async fn calculate_md5(state: State<'_, AppState>) {
//     let input_text = state.input_text.lock().await.clone();
//     let file_path = state.file_path.lock().await.clone();
//     println!("go to here");
//     let result = if !input_text.is_empty() {
//         format!("{:x}", md5::compute(input_text))
//     } else if let Some(path) = file_path {
//         match std::fs::read(path) {
//             Ok(content) => format!("{:x}", md5::compute(content)),
//             Err(_) => "Error reading file".to_string(),
//         }
//     } else {
//         "No input or file provided".to_string()
//     };

//     *state.result_text.lock().await = result;
// }

#[tauri::command]
async fn get_result_text(state: State<'_, AppState>) -> String {
    state.result_text.lock().await.clone()
}

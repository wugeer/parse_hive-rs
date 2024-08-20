#![cfg_attr(
    all(not(debug_assertions), target_os = "windows"),
    windows_subsystem = "windows"
)]

use base64::{engine::general_purpose, Engine as _};
use tauri_demo::HiveSqlParser;

#[tauri::command]
fn gen_all_source_table(input: String, file_content: Option<String>) -> String {
    let mut processor = HiveSqlParser::new();

    let query;
    if !input.is_empty() {
        query = input;
    } else if let Some(base64_content) = file_content {
        match general_purpose::STANDARD.decode(base64_content) {
            Ok(decoded_content) => {
                 // 尝试将 Vec<u8> 转换为 String
    match String::from_utf8(decoded_content) {
        Ok(string) => query = string,
        Err(e) => return format!("Failed to convert: {}", e),
    }
            }
            Err(_) => {
                return "Failed to decode Base64 content".to_string();
            }
        }
    } else {
        return "No input provided".to_string()
    }
    let res = processor.parse(query.as_str());
    if  res.is_ok()  {
        processor.get_table_names().join("\n")
    } else {
        return  format!("error: {:?}", res.err())
    }
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![gen_all_source_table])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

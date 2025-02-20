mod db;
mod table;  
mod page;
mod index;
mod query;  
mod constants;

use crate::db::DB;  
use crate::query::Query;    
use rand::{seq::SliceRandom};
use std::time::Instant;

fn main() {
    let mut db = DB::new();     
    let mut grades_table = db.create_table("Grades".to_string(), 5, 0);  
    let mut query = Query::new(&mut grades_table);  
    let mut keys = Vec::new(); 

    // Inserting 10k records
    let insert_start = Instant::now();
    for i in 0..10_000 {
        query.insert(906_659_671 + i, vec![Some(93), None, None, None, None]);
        keys.push(906_659_671 + i);
    }
    let insert_duration = insert_start.elapsed();
    println!(
        "Inserting 10k records took:  \t\t\t{:?}",
        insert_duration
    );

    // Measuring update performance
    let update_cols = vec![
        vec![None, None, None, None, None],
        vec![None, Some(rand::random::<i32>() % 100), None, None, None],
        vec![None, None, Some(rand::random::<i32>() % 100), None, None],
        vec![None, None, None, Some(rand::random::<i32>() % 100), None],
        vec![None, None, None, None, Some(rand::random::<i32>() % 100)],
    ];

    let update_start = Instant::now();
    for _ in 0..10_000 {
        let key = *keys.choose(&mut rand::thread_rng()).unwrap();
        query.update(key, update_cols.choose(&mut rand::thread_rng()).unwrap().clone());
    }
    let update_duration = update_start.elapsed();
    println!("Updating 10k records took:  \t\t\t{:?}", update_duration);

    // Measuring select performance
    let select_start = Instant::now();
    for _ in 0..10_000 {
        let key = *keys.choose(&mut rand::thread_rng()).unwrap();
        query.select(key, vec![1, 1, 1, 1, 1]);
    }
    let select_duration = select_start.elapsed();
    println!("Selecting 10k records took:  \t\t\t{:?}", select_duration);

    // Measuring aggregate performance
    let agg_start = Instant::now();
    for i in (0..10_000).step_by(100) {
        let start_value = 906_659_671 + i;
        let end_value = start_value + 100;
        let _result = query.sum(start_value, end_value - 1, rand::random::<usize>() % 5);
    }
    let agg_duration = agg_start.elapsed();
    println!("Aggregate 10k of 100 record batch took:\t{:?}", agg_duration);

    // Measuring delete performance
    let delete_start = Instant::now();
    for i in 0..10_000 {
        query.delete(906_659_671 + i);
    }
    let delete_duration = delete_start.elapsed();
    println!("Deleting 10k records took:  \t\t\t{:?}", delete_duration);
}
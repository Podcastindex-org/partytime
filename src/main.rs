use std::fs;
use std::fs::File;
use std::io::BufReader;
use std::error::Error;
use std::mem::drop;
use std::thread::{sleep, spawn};
use std::sync::{Arc,Mutex};
use xml::attribute::OwnedAttribute;
use xml::reader::{EventReader, XmlEvent};


#[derive(Default, Debug)]
struct Feed {
    title : String,
    #[allow(dead_code)]
    description : String,
    #[allow(dead_code)]
    items: Vec<Item>,
    live_items: Vec<LiveItem>,
}

#[derive(Default, Debug)]
struct Item {
    #[allow(dead_code)]
    title : String,
    #[allow(dead_code)]
    description : String,
}

#[derive(Default, Debug)]
struct LiveItem {
    #[allow(dead_code)]
    title : String,
    #[allow(dead_code)]
    description : String,
}

#[derive(Default,Debug)]
struct XmlLocation {
    #[allow(dead_code)]
    path: Vec<String>,
    #[allow(dead_code)]
    context: i32,
    #[allow(dead_code)]
    element: String,
}

fn main() -> std::io::Result<()> {
    let cg_job_count = Arc::new(Mutex::new(0));
    let total_items_parsed = Arc::new(Mutex::new(0));

    let mut cg_parsers = vec![];

    //Get test feeds
    let paths = fs::read_dir("./test_feeds").unwrap();
    for path in paths {
        let file = File::open(path.unwrap().path()).unwrap();
        let file = BufReader::new(file);
        cg_parsers.push(EventReader::new(file));
    }

    //Run each parse in parallel
    for parser in cg_parsers {
        let outstanding_job_count = cg_job_count.clone();
        let mut locked_job_count = outstanding_job_count.lock().unwrap();
        *locked_job_count += 1;
        drop(locked_job_count);

        let total_items_count = total_items_parsed.clone();

        spawn(move || {
            //println!("Parsing xml...", );
            let mut pfeed: Feed = Default::default();
            let mut context = 0;
            let mut inside = vec![];
            for e in parser {
                match e {
                    Ok(
                        XmlEvent::StartElement {
                            name,
                            attributes,
                            namespace: _,
                            ..
                        }
                    ) => {
                        //print element data
                        let mut tag_name = name.local_name;
                        if let Some(p) = name.prefix {
                            tag_name = format!("{}:{}", p, tag_name).to_lowercase().trim().to_string();
                        }
                        if tag_name == "channel" {
                            context = 0;
                        }
                        if context == 0 && tag_name == "item" {
                            context = 1;
                            pfeed.items.push(Item {
                                title: "".to_string(),
                                description: "".to_string(),
                            });
                            let mut locked_items_count = total_items_count.lock().unwrap();
                            *locked_items_count += 1;
                            drop(locked_items_count);
                        }
                        if context == 0 && tag_name == "podcast:liveitem" {
                            context = 2;
                            pfeed.live_items.push(LiveItem {
                                title: "".to_string(),
                                description: "".to_string(),
                            });
                            let mut locked_items_count = total_items_count.lock().unwrap();
                            *locked_items_count += 1;
                            drop(locked_items_count);
                        }
                        inside.push(tag_name);

                        //println!("{} {}", "  ".repeat(inside.len()), inside.last().unwrap());
                        let result = xml_parse_attributes(&attributes);
                        if let Ok(attr) = result.as_ref() {
                            for _a in attr {
                                //println!("{}  --{} = [{}]", "  ".repeat(inside.len()), a.0, a.1);
                            }
                        }
                    }
                    Ok(XmlEvent::CData(data)) => {
                        //println!("{:#?} -> {:#?}", channel_or_item, inside.last().unwrap());

                        let _location = XmlLocation {
                            path: inside.clone(),
                            context,
                            element: data.clone(),
                        };

                        //Channel context
                        if context == 0 && inside.last().unwrap() == "title" {
                            pfeed.title = data.clone().trim().to_string();
                        }
                        if context == 0 && inside.last().unwrap() == "description" {
                            pfeed.description = data.clone().trim().to_string();
                        }

                        //Item context
                        if context == 1 && inside.last().unwrap() == "title" {
                            let pitem = pfeed.items.last_mut().unwrap();
                            pitem.title = data.clone().trim().to_string();
                        }
                        if context == 1 && inside.last().unwrap() == "description" {
                            let pitem = pfeed.items.last_mut().unwrap();
                            pitem.description = data.clone().trim().to_string();
                        }

                        //LiveItem context
                        if context == 2 && inside.last().unwrap() == "title" {
                            let pitem = pfeed.live_items.last_mut().unwrap();
                            pitem.title = data.clone().trim().to_string();
                        }
                        if context == 2 && inside.last().unwrap() == "description" {
                            let pitem = pfeed.live_items.last_mut().unwrap();
                            pitem.description = data.clone().trim().to_string();
                        }
                    }
                    Ok(XmlEvent::Characters(data)) => {
                        //println!("{:#?} -> {:#?}", channel_or_item, inside.last().unwrap());

                        let _location = XmlLocation {
                            path: inside.clone(),
                            context,
                            element: data.clone(),
                        };

                        //Channel context
                        if context == 0 && inside.last().unwrap() == "title" {
                            pfeed.title = data.clone().trim().to_string();
                        }
                        if context == 0 && inside.last().unwrap() == "description" {
                            pfeed.description = data.clone().trim().to_string();
                        }

                        //Item context
                        if context == 1 && inside.last().unwrap() == "title" {
                            let pitem = pfeed.items.last_mut().unwrap();
                            pitem.title = data.clone().trim().to_string();
                        }
                        if context == 1 && inside.last().unwrap() == "description" {
                            let pitem = pfeed.items.last_mut().unwrap();
                            pitem.description = data.clone().trim().to_string();
                        }

                        //LiveItem context
                        if context == 2 && inside.last().unwrap() == "title" {
                            let pitem = pfeed.live_items.last_mut().unwrap();
                            pitem.title = data.clone().trim().to_string();
                        }
                        if context == 2 && inside.last().unwrap() == "description" {
                            let pitem = pfeed.live_items.last_mut().unwrap();
                            pitem.description = data.clone().trim().to_string();
                        }
                    }
                    Ok(XmlEvent::EndElement { name, .. }) => {
                        let mut tag_name = name.local_name;
                        if let Some(p) = name.prefix {
                            tag_name = format!("{}:{}", p, tag_name).to_lowercase().trim().to_string();
                        }
                        if context == 1 && tag_name == "item" {
                            context = 0;
                        }
                        if context == 2 && tag_name == "podcast:liveitem" {
                            context = 0;
                        }
                        inside.pop();
                    }
                    Err(e) => {
                        println!("{:?} -> Error: {:#?}", pfeed.title, e);
                        let mut locked_job_count = outstanding_job_count.lock().unwrap();
                        *locked_job_count -= 1;
                        drop(locked_job_count);
                        break;
                    }
                    Ok(XmlEvent::EndDocument) => {
                        println!(
                            "{:?}[{:?}] -> {:?}",
                            pfeed.title,
                            pfeed.items.len(),
                            pfeed.items.last().unwrap().title
                        );
                        let mut locked_job_count = outstanding_job_count.lock().unwrap();
                        *locked_job_count -= 1;
                        drop(locked_job_count);
                        break;
                    },
                    // There's more: https://docs.rs/xml-rs/latest/xml/reader/enum.XmlEvent.html
                    _ => {}
                }
            }
        });
    }

    let outstanding_job_count = cg_job_count.clone();
    let total_items_count = total_items_parsed.clone();
    loop{
        let locked_job_count = outstanding_job_count.lock().unwrap();
        let locked_items_count = total_items_count.lock().unwrap();
        //println!("[{:?}] waiting...", locked_job_count);
        if *locked_job_count == 0 {
            println!("Done!");
            println!("Total items parsed: {}", locked_items_count);
            break;
        }
        drop(locked_job_count);
        drop(locked_items_count);
        sleep(time::Duration::seconds(1).try_into().unwrap());
    }

    Ok(())
}

fn xml_parse_attributes(attr: &Vec<OwnedAttribute>) -> Result<Vec<(String, String)>, Box<dyn Error>> {

    let mut attributes = vec![];

    for a in attr {
        let mut a_name = a.name.local_name.clone();
        let a_value = a.value.clone();
        if let Some(p) = a.clone().name.prefix {
            a_name = format!("{}:{}", p, a_name);
        }
        attributes.push((a_name.clone(), a_value.clone()));
    }

    return Ok(attributes);
}

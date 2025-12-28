use crate::protocol::{ExecStatus, Job};
use serde_json::{Value, json};
use boa_engine::{Context, Source, JsString, JsValue};
use boa_engine::property::Attribute;
use super::HandlerResult;

pub async fn handle_jmespath(job: &Job) -> HandlerResult {
    let expression = match job.payload.get("expression").and_then(|v| v.as_str()) {
        Some(e) => e,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_EXPRESSION".to_string()),
            Some("Missing 'expression' in payload".to_string())
        ),
    };

    let data = job.payload.get("data").unwrap_or(&Value::Null);

    let expr = match jmespath::compile(expression) {
        Ok(e) => e,
        Err(e) => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("JMESPATH_COMPILE_ERROR".to_string()),
            Some(e.to_string())
        ),
    };

    let result = match expr.search(data) {
         Ok(r) => r,
         Err(e) => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("JMESPATH_RUNTIME_ERROR".to_string()),
            Some(e.to_string())
        ),
    };

    let output_json = serde_json::to_value(&*result).unwrap_or(Value::Null);

    (ExecStatus::Success, job.r#type.clone(), Some(output_json), None, None)
}

pub async fn handle_javascript(job: &Job) -> HandlerResult {
    let code = match job.payload.get("code").and_then(|v| v.as_str()) {
        Some(c) => c,
        None => return (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("MISSING_CODE".to_string()),
            Some("Missing 'code' in payload".to_string())
        ),
    };

    let args = job.payload.get("args").and_then(|v| v.as_object());

    let code = code.to_string();
    let args = args.cloned();
    
    let result = tokio::task::spawn_blocking(move || {
        let mut context = Context::default();
        
        if let Some(args_map) = args {
            for (k, v) in args_map {
                let boa_val = match serde_to_boa(&mut context, v) {
                    Ok(val) => val,
                    Err(e) => return Err(format!("Failed to convert arg {}: {}", k, e)),
                };
                
                let js_key = JsString::from(k.as_str());
                if let Err(e) = context.register_global_property(
                    js_key, 
                    boa_val, 
                    Attribute::WRITABLE | Attribute::ENUMERABLE | Attribute::CONFIGURABLE
                ) {
                    return Err(format!("Failed to register global {}: {}", k, e.to_string()));
                }
            }
        }

        match context.eval(Source::from_bytes(code.as_bytes())) {
            Ok(res) => {
                match boa_to_serde(&mut context, res) {
                    Ok(v) => Ok(v),
                    Err(e) => Err(format!("Failed to convert result: {}", e)),
                }
            },
            Err(e) => Err(format!("Script execution failed: {}", e.to_string())),
        }
    }).await;

    match result {
        Ok(Ok(output)) => (ExecStatus::Success, job.r#type.clone(), Some(output), None, None),
        Ok(Err(err_msg)) => (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("SCRIPT_ERROR".to_string()),
            Some(err_msg)
        ),
        Err(join_err) => (
            ExecStatus::Error,
            job.r#type.clone(),
            None,
            Some("INTERNAL_ERROR".to_string()),
            Some(format!("Tokio join error: {}", join_err))
        ),
    }
}

fn serde_to_boa(context: &mut Context, val: Value) -> Result<JsValue, String> {
    match val {
        Value::Null => Ok(JsValue::null()),
        Value::Bool(b) => Ok(JsValue::new(b)),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(JsValue::new(i))
            } else if let Some(f) = n.as_f64() {
                Ok(JsValue::new(f))
            } else {
                Ok(JsValue::new(0))
            }
        },
        Value::String(s) => Ok(JsValue::new(JsString::from(s.as_str()))),
        Value::Array(arr) => {
            let mut boa_arr = Vec::new();
            for v in arr {
                boa_arr.push(serde_to_boa(context, v)?);
            }
            Ok(JsValue::from(boa_engine::object::builtins::JsArray::from_iter(boa_arr, context)))
        },
        Value::Object(obj) => {
            let js_obj = boa_engine::object::JsObject::with_object_proto(context.intrinsics());
            for (k, v) in obj {
                let val = serde_to_boa(context, v)?;
                js_obj.set(JsString::from(k.as_str()), val, false, context).map_err(|e| e.to_string())?;
            }
            Ok(JsValue::from(js_obj))
        }
    }
}

fn boa_to_serde(context: &mut Context, val: JsValue) -> Result<Value, String> {
    if val.is_null() || val.is_undefined() {
        return Ok(Value::Null);
    } else if let Some(b) = val.as_boolean() {
        return Ok(Value::Bool(b));
    } else if let Some(n) = val.as_number() {
         if n.fract() == 0.0 {
             return Ok(json!(n as i64));
         }
         return Ok(json!(n));
    } else if let Some(s) = val.as_string() {
         return Ok(Value::String(s.to_std_string_escaped()));
    } else if let Some(obj) = val.as_object() {
         if obj.is_array() {
             let len_val = obj.get(JsString::from("length"), context).map_err(|e| e.to_string())?;
             let len = len_val.as_number().unwrap_or(0.0) as u32;
             let mut arr = Vec::new();
             for i in 0..len {
                 let val = obj.get(i, context).map_err(|e| e.to_string())?;
                 arr.push(boa_to_serde(context, val)?);
             }
             Ok(Value::Array(arr))
         } else if obj.is_callable() {
             Ok(Value::String("[Function]".to_string()))
         } else {
             let json_obj = context.global_object().get(JsString::from("JSON"), context).map_err(|e| e.to_string())?;
             let stringify = json_obj.as_object().unwrap().get(JsString::from("stringify"), context).map_err(|e| e.to_string())?;
             if let Some(func) = stringify.as_callable() {
                  let json_str_val = func.call(&JsValue::undefined(), &[val.clone()], context).map_err(|e| e.to_string())?;
                  if let Some(s) = json_str_val.as_string() {
                       let s_str = s.to_std_string_escaped();
                       let v: Value = serde_json::from_str(&s_str).map_err(|e| e.to_string())?;
                       return Ok(v);
                  }
             }
             Ok(Value::String("[Object]".to_string()))
         }
    } else {
        Ok(Value::String(format!("{:?}", val)))
    }
}

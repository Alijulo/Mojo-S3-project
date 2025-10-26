// use axum::{
//     http::{Request, StatusCode},
//     middleware::{self, Next},
//     response::{IntoResponse, Response},
//     Extension,
// };
// use sqlx::SqlitePool;
// use crate::s3_operations::auth::{authenticate_request, AuthenticatedUser, AppError};

// // Middleware to authenticate requests and insert AuthenticatedUser into extensions
// pub async fn auth_middleware<B>(
//     Extension(pool): Extension<SqlitePool>,
//     mut request: Request<B>,
//     next: Next<B>,
// ) -> Result<Response, Response> {
//     match authenticate_request(request.headers(), &pool).await {
//         Ok(user) => {
//             // Insert AuthenticatedUser into request extensions
//             request.extensions_mut().insert(user);
//             Ok(next.run(request).await)
//         }
//         Err(e) => {
//             tracing::error!("Authentication failed: {:?}", e);
//             Err((StatusCode::UNAUTHORIZED, format!("{:?}", e)).into_response())
//         }
//     }
// }
use crate::services::session::SessionService;
use tonic::Status;
use tonic::codegen::BoxFuture;
use tonic::codegen::http::HeaderMap;
use tower::Service;
use uuid::Uuid;
use wakuwaku::sqlx::DatabaseProcessor;

#[derive(Clone)]
pub struct AuthLayer {
    pub service: SessionService,
}

impl<S> tower::Layer<S> for AuthLayer {
    type Service = AuthMiddleware<S>;
    fn layer(&self, service: S) -> Self::Service {
        AuthMiddleware {
            inner: service,
            service: self.service.clone(),
        }
    }
}

#[derive(Clone)]
pub struct AuthMiddleware<S> {
    inner: S,
    service: SessionService,
}

impl<S, ReqBody, ResBody> Service<tonic::codegen::http::Request<ReqBody>> for AuthMiddleware<S>
where
    S: Service<
            tonic::codegen::http::Request<ReqBody>,
            Response = tonic::codegen::http::Response<ResBody>,
        > + Clone
        + Send
        + 'static,
    S::Future: Send + 'static,
    ReqBody: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = BoxFuture<Self::Response, Self::Error>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut req: tonic::codegen::http::Request<ReqBody>) -> Self::Future {
        let session_service = self.service.clone();
        let inner_clone = self.inner.clone();
        let mut inner = std::mem::replace(&mut self.inner, inner_clone);
        Box::pin(async move {
            if let Ok(session_info) = user_auth(req.headers(), &session_service).await {
                req.extensions_mut().insert(session_info);
            }
            inner.call(req).await
        })
    }
}

pub const SESSION_ID_HEADER: &str = "x-session-id";

#[derive(Clone)]
pub struct UserSessionInfo {
    pub user_id: Uuid,
    pub session_id: String,
}

async fn user_auth(
    metadata: &HeaderMap,
    service: &SessionService,
) -> Result<UserSessionInfo, Status> {
    todo!()
}

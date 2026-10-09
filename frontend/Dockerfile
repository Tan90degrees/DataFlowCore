FROM nginxinc/nginx-unprivileged:1.28-alpine
COPY nginx.conf.template /etc/nginx/templates/default.conf.template
COPY index.html styles.css app.js api.js dag.js config.js /usr/share/nginx/html/
ENV DATAFLOW_API_UPSTREAM=http://control:8080
EXPOSE 8080

-- Таблица заявок колл-центра в PostgreSQL (источник)
CREATE TABLE IF NOT EXISTS call_center_requests (
    id SERIAL PRIMARY KEY,
    client_phone VARCHAR(20) NOT NULL,
    operator_id INTEGER NOT NULL,
    status VARCHAR(20) NOT NULL 
        CHECK (status IN ('new', 'in_progress', 'closed', 'rejected')),
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Функция для автоматического обновления updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_updated_at
    BEFORE UPDATE ON call_center_requests
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

package org.example.node;

public enum NodeRole {
    MASTER,   // Главный узел (управляет игрой)
    DEPUTY,   // Заместитель (становится MASTER при отвале)
    NORMAL,   // Обычный игрок
    VIEWER    // Наблюдатель (не играет)
}

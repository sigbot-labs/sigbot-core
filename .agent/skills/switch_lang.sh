#!/bin/bash

# Cursor Skills Switch Language Script
# Usage: ./switch_lang.sh [zh|en|status]

SKILLS_DIR="$(cd "$(dirname "$0")" && pwd)"
ACTION="${1:-status}"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Detect current language from symlink target
detect_lang() {
    [ -L "$1/SKILL.md" ] && basename "$(readlink "$1/SKILL.md")" | grep -q "_ZH" && echo "zh" || echo "en"
}

# Switch language
switch_lang() {
    local dir="$1"
    local target="$2"
    local name=$(basename "$dir")
    
    [ ! -f "$dir/$target" ] && echo -e "${YELLOW}⚠️  $name: $target not found${NC}" && return 1
    
    rm -f "$dir/SKILL.md"
    ln -snf "$target" "$dir/SKILL.md"
    echo -e "${GREEN}✓${NC} $name"
}

# Show status
show_status() {
    echo -e "${BLUE}📊 Language Status${NC}"
    echo "=================="
    
    local zh=0 en=0
    for dir in "$SKILLS_DIR"/*/; do
        [ -d "$dir" ] || continue
        local name=$(basename "$dir")
        local lang=$(detect_lang "$dir")
        
        if [ "$lang" = "zh" ]; then
            echo -e "${GREEN}✓${NC} $name: 中文"
            zh=$((zh + 1))
        else
            echo -e "${BLUE}✓${NC} $name: English"
            en=$((en + 1))
        fi
    done
    
    echo "=================="
    echo -e "Total: $((zh + en)) | ${GREEN}中文: $zh${NC} | ${BLUE}English: $en${NC}"
}

# Main
case "$ACTION" in
    zh|chinese|中文)
        echo -e "${BLUE}🔄 Switching to Chinese...${NC}\n"
        for dir in "$SKILLS_DIR"/*/; do [ -d "$dir" ] && switch_lang "$dir" "SKILL_ZH.md"; done
        ;;
    en|english|英文)
        echo -e "${BLUE}🔄 Switching to English...${NC}\n"
        for dir in "$SKILLS_DIR"/*/; do [ -d "$dir" ] && switch_lang "$dir" "SKILL_EN.md"; done
        ;;
    status|stat)
        show_status
        ;;
    help|-h|--help)
        echo "Usage: $0 [zh|en|status]"
        echo "  zh     - Switch to Chinese"
        echo "  en     - Switch to English"
        echo "  status - Show status (default)"
        ;;
    *)
        echo -e "${RED}❌ Unknown: $ACTION${NC}"
        echo "Use: $0 help"
        exit 1
        ;;
esac

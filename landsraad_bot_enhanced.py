# landsraad_bot_enhanced.py - Enhanced Discord Bot with Button-Based Claiming System
import discord
from discord import app_commands
from discord.ext import commands, tasks
import sqlite3
import asyncio
from datetime import datetime, timedelta, time
import json
import os
from typing import Optional, List
import csv
import io
import pytz
from dotenv import load_dotenv
import logging
import threading
from contextlib import contextmanager

# Load environment variables
load_dotenv()

# Bot configuration
BOT_PREFIX = '!'
DATABASE = 'data/landsraad.db'
EMBED_COLOR = 0xD4AF37  # Gold color
GOAL_AMOUNT = 70000
TOKEN = os.getenv('DISCORD_BOT_TOKEN')

# Alliance constants
ATREIDES = "Atreides"
HARKONNEN = "Harkonnen"
OUR_ALLIANCE = HARKONNEN  # We are Harkonnen

# Schedule configuration
SCHEDULE_CHANNEL = "weeklyschedule"  # Channel name for automatic posts
PST = pytz.timezone('US/Pacific')  # North America Pacific timezone
SCHEDULE_CHANNEL_ID = None  # Will be set by /set_schedule_channel

# Database connection management and optimization
class DatabaseManager:
    """Thread-safe database connection manager with connection pooling."""
    
    def __init__(self, database_path: str, max_connections: int = 10):
        self.database_path = database_path
        self.max_connections = max_connections
        self._connections = []
        self._lock = threading.Lock()
        
    @contextmanager
    def get_connection(self):
        """Get a database connection from the pool."""
        conn = None
        try:
            with self._lock:
                if self._connections:
                    conn = self._connections.pop()
                else:
                    conn = sqlite3.connect(
                        self.database_path,
                        timeout=30.0,
                        check_same_thread=False
                    )
                    # Optimize SQLite settings
                    conn.execute('PRAGMA journal_mode=WAL')
                    conn.execute('PRAGMA synchronous=NORMAL')
                    conn.execute('PRAGMA cache_size=10000')
                    conn.execute('PRAGMA temp_store=MEMORY')
            
            yield conn
            
        finally:
            if conn:
                with self._lock:
                    if len(self._connections) < self.max_connections:
                        self._connections.append(conn)
                    else:
                        conn.close()
    
    def close_all(self):
        """Close all connections in the pool."""
        with self._lock:
            for conn in self._connections:
                conn.close()
            self._connections.clear()

# Initialize database manager
db_manager = DatabaseManager(DATABASE)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('data/bot.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('LandsraadBot')

# Initialize bot with hybrid commands and optimized syncing
class LandsraadBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.guilds = True
        super().__init__(
            command_prefix=BOT_PREFIX, 
            intents=intents,
            max_messages=None,  # Optimize memory usage
            help_command=None   # Disable default help command
        )
        self.synced = False
        self.guild_sync_complete = set()
        
    async def setup_hook(self):
        """Setup hook called when bot starts. Optimized for faster command syncing."""
        print("Setting up bot...")
        
        # Add persistent views first
        self.add_view(LandsraadView())
        
        # Initialize databases before syncing commands
        init_database()
        init_database_locations()
        
        print("Bot setup complete. Command syncing will happen after ready event.")
    
    async def sync_commands_optimized(self, guild_id: int = None):
        """Optimized command syncing - can sync to specific guild for instant updates."""
        try:
            if guild_id:
                # Guild-specific sync (instant update)
                guild = discord.Object(id=guild_id)
                synced = await self.tree.sync(guild=guild)
                print(f"Synced {len(synced)} commands to guild {guild_id} (instant)")
                self.guild_sync_complete.add(guild_id)
                return len(synced)
            else:
                # Global sync (takes up to 1 hour to propagate)
                if not self.synced:
                    synced = await self.tree.sync()
                    self.synced = True
                    print(f"Synced {len(synced)} commands globally (may take up to 1 hour)")
                    return len(synced)
                else:
                    print("Commands already synced globally")
                    return 0
        except Exception as e:
            print(f"Failed to sync commands: {e}")
            return 0

bot = LandsraadBot()

# Database functions
def init_database():
    """Initialize the database with required tables and handle migrations."""
    os.makedirs('data', exist_ok=True)
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # Enhanced houses table with alliance
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS houses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE NOT NULL,
            quest TEXT DEFAULT 'Unknown',
            current_goal INTEGER DEFAULT 0,
            goal INTEGER DEFAULT 70000,
            points_per_delivery INTEGER DEFAULT 1,
            is_locked BOOLEAN DEFAULT 1,
            completed_by TEXT DEFAULT NULL,
            notes TEXT DEFAULT NULL,
            desert_location TEXT DEFAULT NULL,
            alliance TEXT DEFAULT NULL,
            deep_desert_cp INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_by TEXT DEFAULT 'System'
        )
        ''')
        
        # Check for new columns and add if missing
        cursor.execute("PRAGMA table_info(houses)")
        columns = [column[1] for column in cursor.fetchall()]
        
        if 'alliance' not in columns:
            cursor.execute('ALTER TABLE houses ADD COLUMN alliance TEXT DEFAULT NULL')
        
        if 'deep_desert_cp' not in columns:
            cursor.execute('ALTER TABLE houses ADD COLUMN deep_desert_cp INTEGER DEFAULT 0')
        
        # Weekly reset tracking
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS reset_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            reset_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            reset_by TEXT,
            houses_reset INTEGER,
            houses_completed INTEGER
        )
        ''')
        
        # Individual contribution tracking
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS contributions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            house_id INTEGER,
            user_id TEXT,
            user_name TEXT,
            amount INTEGER,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (house_id) REFERENCES houses (id)
        )
        ''')
        
        conn.commit()

def init_database_locations():
    """Add location tracking tables to the database."""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # Deep Desert sectors table (81 sectors)
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS deep_desert_sectors (
            sector_id TEXT PRIMARY KEY,  -- Format: "A1", "B2", etc.
            row_letter TEXT NOT NULL,
            col_number INTEGER NOT NULL,
            survey_status TEXT DEFAULT 'unsurveyed',  -- unsurveyed, partial, complete
            last_surveyed TIMESTAMP,
            surveyed_by TEXT,
            notes TEXT
        )
        ''')
        
        # Guild bases table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS guild_bases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_name TEXT NOT NULL,
            sector_id TEXT NOT NULL,
            coordinates TEXT,  -- More specific coords within sector
            base_type TEXT,  -- main, outpost, temporary
            alliance TEXT,  -- Atreides, Harkonnen, etc.
            discovered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            discovered_by TEXT,
            is_active BOOLEAN DEFAULT 1,
            notes TEXT,
            FOREIGN KEY (sector_id) REFERENCES deep_desert_sectors (sector_id)
        )
        ''')
        
        # Spice locations table
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS spice_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sector_id TEXT NOT NULL,
            spice_type TEXT NOT NULL,  -- blow, crater, field
            size TEXT,  -- small, medium, large, massive
            coordinates TEXT,
            discovered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            discovered_by TEXT,
            last_verified TIMESTAMP,
            is_depleted BOOLEAN DEFAULT 0,
            estimated_yield INTEGER,
            notes TEXT,
            FOREIGN KEY (sector_id) REFERENCES deep_desert_sectors (sector_id)
        )
        ''')
        
        # Landsraad capture points
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS landsraad_points (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sector_id TEXT NOT NULL,
            point_name TEXT,
            current_controller TEXT,  -- Guild/alliance controlling it
            coordinates TEXT,
            tier INTEGER DEFAULT 1,  -- 1-3 for importance
            last_captured TIMESTAMP,
            captured_by TEXT,
            defense_rating INTEGER,  -- 1-10 scale
            notes TEXT,
            FOREIGN KEY (sector_id) REFERENCES deep_desert_sectors (sector_id)
        )
        ''')
        
        # Resource concentrations
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS resource_locations (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            sector_id TEXT NOT NULL,
            resource_type TEXT NOT NULL,  -- water, minerals, salvage, etc.
            concentration TEXT,  -- low, medium, high, extreme
            coordinates TEXT,
            discovered_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            discovered_by TEXT,
            last_verified TIMESTAMP,
            is_exhausted BOOLEAN DEFAULT 0,
            extraction_difficulty INTEGER,  -- 1-10 scale
            notes TEXT,
            FOREIGN KEY (sector_id) REFERENCES deep_desert_sectors (sector_id)
        )
        ''')
        
        # Populate all 81 sectors
        for row in range(9):  # A-I
            for col in range(1, 10):  # 1-9
                sector_id = f"{chr(65 + row)}{col}"  # A1, A2, ..., I9
                cursor.execute('''
                INSERT OR IGNORE INTO deep_desert_sectors (sector_id, row_letter, col_number)
                VALUES (?, ?, ?)
                ''', (sector_id, chr(65 + row), col))
        
        # Add channel configuration table for auto-updates
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS channel_config (
            config_name TEXT PRIMARY KEY,
            channel_id TEXT,
            message_id TEXT,
            guild_id TEXT
        )
        ''')
        
        conn.commit()

def populate_initial_houses():
    """Populate the database with the 25 Landsraad houses."""
    houses = [
        'Alexin', 'Argosaz', 'Dyvets', 'Ecaz', 'Hagal', 'Hurata',
        'Imota', 'Kenola', 'Lindaren', 'Maros', 'Mikarrol', 'Moritani', 'Mutelli',
        'Novebruns', 'Richese', 'Sor', 'Spinnette', 'Taligari', 'Thorvald',
        'Tseida', 'Varota', 'Vernius', 'Wallach', 'Wayku', 'Wydras'
    ]
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # First, remove any houses not in our list (like Harkonnen if it exists)
        cursor.execute('DELETE FROM houses WHERE name NOT IN ({})'.format(','.join('?' * len(houses))), houses)
        
        # Then insert the 25 houses
        for house in houses:
            cursor.execute('''
            INSERT OR IGNORE INTO houses (name) VALUES (?)
            ''', (house,))
        
        conn.commit()

def get_house_data(house_name: str):
    """Get data for a specific house."""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('''
        SELECT * FROM houses WHERE LOWER(name) = LOWER(?)
        ''', (house_name,))
        result = cursor.fetchone()
    return result

def get_all_houses():
    """Get all houses in alphabetical order."""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM houses ORDER BY name')
        houses = cursor.fetchall()
    return houses

def update_house_data(house_name: str, field: str, value, updated_by: str):
    """Update a specific field for a house."""
    allowed_fields = ['quest', 'current_goal', 'points_per_delivery', 'is_locked', 
                     'completed_by', 'notes', 'desert_location', 'alliance', 'deep_desert_cp']
    if field not in allowed_fields:
        return False
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        query = f'''
        UPDATE houses 
        SET {field} = ?, last_updated = CURRENT_TIMESTAMP, updated_by = ?
        WHERE LOWER(name) = LOWER(?)
        '''
        
        cursor.execute(query, (value, updated_by, house_name))
        success = cursor.rowcount > 0
        conn.commit()
    return success

# Weekly Schedule Functions
def get_next_weekday(target_weekday: int, target_hour: int, target_minute: int = 0) -> datetime:
    """Get the next occurrence of a specific weekday and time.
    
    Args:
        target_weekday: 0=Monday, 1=Tuesday, 2=Wednesday, etc.
        target_hour: Hour in 24-hour format
        target_minute: Minute
    
    Returns:
        datetime object in PST timezone
    """
    now = datetime.now(PST)
    
    # Calculate days until target weekday
    days_ahead = target_weekday - now.weekday()
    if days_ahead < 0:  # Target day already happened this week
        days_ahead += 7
    elif days_ahead == 0:  # Target day is today
        target_time = now.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
        if now >= target_time:  # Target time already passed today
            days_ahead = 7
    
    target_date = now + timedelta(days=days_ahead)
    target_datetime = target_date.replace(hour=target_hour, minute=target_minute, second=0, microsecond=0)
    
    return target_datetime

def calculate_schedule_events() -> dict:
    """Calculate the next occurrences of all weekly events."""
    events = {}
    
    # Coriolis Storm: Monday 5PM to Tuesday 3AM
    coriolis_start = get_next_weekday(0, 17)  # Monday 5PM
    coriolis_end = coriolis_start + timedelta(hours=10)  # Tuesday 3AM (10 hours later)
    
    # Landsraad New Term: Tuesday 3AM (same as Coriolis end)
    landsraad_new_term = coriolis_end
    
    # Landsraad Voting: Saturday 6PM to Sunday 6PM
    voting_start = get_next_weekday(5, 18)  # Saturday 6PM
    voting_end = voting_start + timedelta(hours=24)  # Sunday 6PM (24 hours later)
    
    events = {
        'coriolis_start': coriolis_start,
        'coriolis_end': coriolis_end,
        'landsraad_new_term': landsraad_new_term,
        'voting_start': voting_start,
        'voting_end': voting_end
    }
    
    return events

def create_schedule_embed() -> discord.Embed:
    """Create the weekly schedule embed with dynamic timestamps."""
    events = calculate_schedule_events()
    
    embed = discord.Embed(
        title="🌌 **DUNE Awakening - North America Weekly Schedule**",
        description="All times shown in your local timezone",
        color=0xD4AF37  # Gold color
    )
    
    # Convert to Unix timestamps for Discord
    coriolis_start_ts = int(events['coriolis_start'].timestamp())
    coriolis_end_ts = int(events['coriolis_end'].timestamp())
    landsraad_new_ts = int(events['landsraad_new_term'].timestamp())
    voting_start_ts = int(events['voting_start'].timestamp())
    voting_end_ts = int(events['voting_end'].timestamp())
    
    # Coriolis Storm section
    embed.add_field(
        name="⚡ **Coriolis Storm**",
        value=f"**Starts:** <t:{coriolis_start_ts}:F> (<t:{coriolis_start_ts}:R>)\n"
              f"**Ends:** <t:{coriolis_end_ts}:F> (<t:{coriolis_end_ts}:R>)",
        inline=False
    )
    
    # Landsraad section
    embed.add_field(
        name="🏛️ **Landsraad**",
        value=f"**New term starts:** <t:{landsraad_new_ts}:F> (<t:{landsraad_new_ts}:R>)\n"
              f"**Voting session starts:** <t:{voting_start_ts}:F> (<t:{voting_start_ts}:R>)\n"
              f"**Voting session ends:** <t:{voting_end_ts}:F> (<t:{voting_end_ts}:R>)",
        inline=False
    )
    
    # Add helpful info
    embed.add_field(
        name="ℹ️ **Info**",
        value="• Schedule updates automatically each week\n"
              "• Times shown in your local timezone\n"
              "• Use `/weeklyschedule` to view anytime",
        inline=False
    )
    
    embed.set_footer(text=f"Generated: {datetime.now(PST).strftime('%Y-%m-%d %I:%M %p PST')}")
    
    return embed

# Store the last posted message ID for editing/deleting
last_schedule_message_id = None
last_schedule_channel_id = None

def claim_house_for_alliance(house_name: str, alliance: str, claimed_by: str):
    """Claim a house for a specific alliance."""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # When claiming for an alliance, ONLY set alliance field
        # Do NOT set completed_by - that's only for when goal is reached
        cursor.execute('''
        UPDATE houses 
        SET alliance = ?, last_updated = CURRENT_TIMESTAMP, updated_by = ?
        WHERE LOWER(name) = LOWER(?)
        ''', (alliance, claimed_by, house_name))
        
        success = cursor.rowcount > 0
        
        # Debug: Let's check what actually got saved
        if success:
            cursor.execute('SELECT name, alliance FROM houses WHERE LOWER(name) = LOWER(?)', (house_name,))
            result = cursor.fetchone()
            print(f"DEBUG: Set house {result[0]} alliance to: {result[1]}")
        
        conn.commit()
    return success

# House Action View - Shows when you click a house
class HouseActionView(discord.ui.View):
    def __init__(self, house_name: str, house_data: tuple):
        super().__init__(timeout=300)  # 5 minute timeout
        self.house_name = house_name
        self.house_data = house_data
        
        # Extract house info
        is_locked = house_data[6] if len(house_data) > 6 else True
        alliance = house_data[10] if len(house_data) > 10 else None
        
        # If house is locked, show unlock button instead
        if is_locked:
            unlock_button = discord.ui.Button(
                style=discord.ButtonStyle.secondary,
                emoji="🔓",
                label="Unlock House",
                custom_id="unlock_house"
            )
            unlock_button.callback = self.unlock_house_callback
            self.add_item(unlock_button)
        else:
            # Add Update button
            update_button = discord.ui.Button(
                style=discord.ButtonStyle.primary,
                emoji="✏️",
                label="Update House",
                custom_id="update_house"
            )
            update_button.callback = self.update_house_callback
            self.add_item(update_button)
            
            # Add Claim for Atreides button (disable if already claimed by Atreides)
            atreides_button = discord.ui.Button(
                style=discord.ButtonStyle.success,
                emoji="🟢",
                label="Claim for Atreides",
                custom_id="claim_atreides",
                disabled=(alliance == ATREIDES)
            )
            atreides_button.callback = self.claim_atreides_callback
            self.add_item(atreides_button)
            
            # Add Claim for Harkonnen button (disable if already claimed by Harkonnen)
            harkonnen_button = discord.ui.Button(
                style=discord.ButtonStyle.danger,
                emoji="🔴",
                label="Claim for Harkonnen",
                custom_id="claim_harkonnen",
                disabled=(alliance == HARKONNEN)
            )
            harkonnen_button.callback = self.claim_harkonnen_callback
            self.add_item(harkonnen_button)
            
            # Add Unclaim button if house is currently claimed
            if alliance:
                unclaim_button = discord.ui.Button(
                    style=discord.ButtonStyle.secondary,
                    emoji="🔄",
                    label="Unclaim House",
                    custom_id="unclaim_house"
                )
                unclaim_button.callback = self.unclaim_house_callback
                self.add_item(unclaim_button)
        
        # Add Cancel button
        cancel_button = discord.ui.Button(
            style=discord.ButtonStyle.secondary,
            emoji="❌",
            label="Cancel",
            custom_id="cancel"
        )
        cancel_button.callback = self.cancel_callback
        self.add_item(cancel_button)
    
    async def unlock_house_callback(self, interaction: discord.Interaction):
        """Show unlock modal."""
        # FIXED: Use interaction instead of non-existent self.parent_interaction
        modal = UnlockHouseModal(self.house_name, interaction)
        await interaction.response.send_modal(modal)
    
    async def update_house_callback(self, interaction: discord.Interaction):
        """Show update modal."""
        modal = UpdateHouseModal(self.house_name, self.house_data)
        await interaction.response.send_modal(modal)
    
    async def claim_atreides_callback(self, interaction: discord.Interaction):
        """Claim house for Atreides."""
        success = claim_house_for_alliance(self.house_name, ATREIDES, str(interaction.user))
        
        if success:
            # Get updated house data
            house_data = get_house_data(self.house_name)
            embed = create_house_info_embed(self.house_name, house_data)
            
            # Update the action message
            await interaction.response.edit_message(
                content=f"🟢 **House {self.house_name} has been claimed by {ATREIDES}!**",
                embed=embed,
                view=None  # Remove buttons
            )
        else:
            await interaction.response.send_message("❌ Failed to claim house.", ephemeral=True)
    
    async def claim_harkonnen_callback(self, interaction: discord.Interaction):
        """Claim house for Harkonnen."""
        success = claim_house_for_alliance(self.house_name, HARKONNEN, str(interaction.user))
        
        if success:
            # Get updated house data
            house_data = get_house_data(self.house_name)
            embed = create_house_info_embed(self.house_name, house_data)
            
            # Update the action message
            await interaction.response.edit_message(
                content=f"🔴 **House {self.house_name} has been claimed by {HARKONNEN}!**",
                embed=embed,
                view=None  # Remove buttons
            )
        else:
            await interaction.response.send_message("❌ Failed to claim house.", ephemeral=True)
    
    async def unclaim_house_callback(self, interaction: discord.Interaction):
        """Remove claim from house."""
        success = claim_house_for_alliance(self.house_name, None, str(interaction.user))
        
        if success:
            # Get updated house data
            house_data = get_house_data(self.house_name)
            embed = create_house_info_embed(self.house_name, house_data)
            
            # Update the action message
            await interaction.response.edit_message(
                content=f"🔄 **House {self.house_name} has been unclaimed!**",
                embed=embed,
                view=None  # Remove buttons
            )
        else:
            await interaction.response.send_message("❌ Failed to unclaim house.", ephemeral=True)
    
    async def cancel_callback(self, interaction: discord.Interaction):
        """Cancel and close the action view."""
        await interaction.response.edit_message(
            content="❌ **Action cancelled.**",
            embed=None,
            view=None
        )

# Unlock House Modal
class UnlockHouseModal(discord.ui.Modal):
    def __init__(self, house_name: str, parent_interaction=None):
        super().__init__(title=f"Unlock House {house_name}")
        self.house_name = house_name
        self.parent_interaction = parent_interaction
        
        # Quest input
        self.quest_input = discord.ui.TextInput(
            label="Quest",
            placeholder="Enter the kill or delivery quest",
            max_length=100,
            required=True
        )
        self.add_item(self.quest_input)
        
        # Points per delivery input
        self.ppd_input = discord.ui.TextInput(
            label="Points Per Delivery",
            placeholder="23",
            max_length=10,
            required=True
        )
        self.add_item(self.ppd_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            # Parse points per delivery
            ppd = int(self.ppd_input.value.strip())
            
            # Unlock the house and set initial values
            update_house_data(self.house_name, 'is_locked', 0, str(interaction.user))
            update_house_data(self.house_name, 'quest', self.quest_input.value.strip(), str(interaction.user))
            update_house_data(self.house_name, 'points_per_delivery', ppd, str(interaction.user))
            
            # Show house info
            house_data = get_house_data(self.house_name)
            embed = create_house_info_embed(self.house_name, house_data)
            
            await interaction.response.edit_message(
                content=f"🔓 **House {self.house_name} has been unlocked!**",
                embed=embed,
                view=None
            )
            
            # Note: Auto-refresh removed - users should use /refresh_panel or /landsraad
            
        except ValueError:
            await interaction.response.send_message(
                "❌ Invalid Points Per Delivery value. Please enter a number.",
                ephemeral=True
            )
        except Exception as e:
            await interaction.response.send_message(
                f"❌ An error occurred: {str(e)}",
                ephemeral=True
            )

# Update House Modal - SIMPLIFIED! Only Current Goal and Deep Desert CP
class UpdateHouseModal(discord.ui.Modal):
    def __init__(self, house_name: str, house_data: tuple):
        super().__init__(title=f"Update House {house_name}")
        self.house_name = house_name
        self.house_data = house_data
        
        # Extract current values with safe defaults
        current_goal = house_data[3] if len(house_data) > 3 else 0
        deep_desert_cp = house_data[11] if len(house_data) > 11 else 0
        
        # Current goal input
        self.current_goal_input = discord.ui.TextInput(
            label="Current Goal Amount",
            placeholder="50000",
            default=str(current_goal),
            max_length=10,
            required=False
        )
        self.add_item(self.current_goal_input)
        
        # Deep desert CP input
        self.deep_desert_input = discord.ui.TextInput(
            label="Deep Desert Control Point",
            placeholder="0",
            default=str(deep_desert_cp),
            max_length=10,
            required=False
        )
        self.add_item(self.deep_desert_input)
    
    async def on_submit(self, interaction: discord.Interaction):
        try:
            updates_made = []
            
            # Update current goal if provided
            if self.current_goal_input.value.strip():
                try:
                    current_goal = int(self.current_goal_input.value.strip().replace(',', ''))
                    update_house_data(self.house_name, 'current_goal', current_goal, str(interaction.user))
                    updates_made.append(f"Current Goal → {current_goal:,}")
                except ValueError:
                    pass
            
            # Update deep desert CP if provided
            if self.deep_desert_input.value.strip():
                try:
                    deep_desert_cp = int(self.deep_desert_input.value.strip().replace(',', ''))
                    update_house_data(self.house_name, 'deep_desert_cp', deep_desert_cp, str(interaction.user))
                    updates_made.append(f"Deep Desert CP → {deep_desert_cp}")
                except ValueError:
                    pass
            
            # Show updated house info
            house_data = get_house_data(self.house_name)
            embed = create_house_info_embed(self.house_name, house_data)
            
            update_text = "\n".join(updates_made) if updates_made else "No changes made"
            await interaction.response.edit_message(
                content=f"✅ **House {self.house_name} updated!**\n{update_text}\n\n💡 *Use `/refresh_panel` or `/landsraad` to see the updated main panel.*",
                embed=embed,
                view=None
            )
            
        except Exception as e:
            await interaction.response.send_message(
                f"❌ An error occurred: {str(e)}",
                ephemeral=True
            )

# Create house info embed (preview after update)
def create_house_info_embed(house_name: str, house_data: tuple):
    """Create detailed house information embed."""
    # Unpack data with safe defaults
    house_id = house_data[0]
    name = house_data[1]
    quest = house_data[2] if len(house_data) > 2 else "Unknown"
    current = house_data[3] if len(house_data) > 3 else 0
    goal = house_data[4] if len(house_data) > 4 else GOAL_AMOUNT
    ppd = house_data[5] if len(house_data) > 5 else 1
    is_locked = house_data[6] if len(house_data) > 6 else True
    completed_by = house_data[7] if len(house_data) > 7 else None
    notes = house_data[8] if len(house_data) > 8 else None
    desert_location = house_data[9] if len(house_data) > 9 else None
    alliance = house_data[10] if len(house_data) > 10 else None
    deep_desert_cp = house_data[11] if len(house_data) > 11 else 0
    updated_by = house_data[13] if len(house_data) > 13 else "Unknown"
    
    # Debug print
    print(f"DEBUG: House {name} - Alliance field value: '{alliance}'")
    
    # Determine embed color based on status
    if alliance == ATREIDES:
        color = 0x57F287  # Green
        status_emoji = "🟢"
    elif alliance == HARKONNEN:
        color = 0xED4245  # Red
        status_emoji = "🔴"
    else:
        color = 0x5865F2  # Blue
        status_emoji = "🔓"
    
    embed = discord.Embed(
        title=f"House {house_name}",
        color=color
    )
    
    # Status
    progress_pct = (current / goal) * 100 if goal > 0 else 0
    if is_locked:
        status_text = f"🔒 Locked"
    elif alliance:
        status_text = f"{status_emoji} Claimed by {alliance}"
    else:
        status_text = f"{status_emoji} In Prog ({progress_pct:.1f}%)"
    
    embed.add_field(name="Status", value=status_text, inline=True)
    
    # Quest
    embed.add_field(name="📜 Quest", value=quest or "Not set", inline=True)
    
    # Progress
    progress_text = f"Cur: {current:,}\nGoal: {goal:,}\nRem: {max(0, goal - current):,}"
    embed.add_field(name="📊 Progress", value=progress_text, inline=True)
    
    # Deliveries
    if alliance and current < goal:
        # House was claimed before we reached goal
        deliveries_text = f"PPD: {ppd}\nClaimed before completion"
    else:
        turns_needed = max(1, -(-max(0, goal - current) // ppd)) if ppd > 0 else "∞"
        deliveries_text = f"PPD: {ppd}\nTurns: {turns_needed}"
    embed.add_field(name="🚚 Deliveries", value=deliveries_text, inline=True)
    
    # Alliance - FIXED to show the actual alliance value
    embed.add_field(name="🏛️ Alliance", value=alliance or "Unclaimed", inline=True)
    
    # Deep Desert CP
    embed.add_field(name="🏜️ Deep Desert CP", value=str(deep_desert_cp), inline=True)
    
    # Progress bar
    progress_bar = create_progress_bar(current, goal)
    embed.add_field(name="Progress Bar", value=f"```{progress_bar}```", inline=False)
    
    # Rewards
    rewards_text = "💎 **700:** 31\n💎 **3,500:** 153\n💎 **7,000:** 306\n💎 **10,500:** 457\n👑 **14,000:** 609"
    embed.add_field(name="🎁 Rewards", value=rewards_text, inline=False)
    
    embed.set_footer(text=f"Last by {updated_by} • Today at {datetime.now().strftime('%I:%M %p')}")
    
    return embed

def create_progress_bar(current: int, max_val: int) -> str:
    """Create a visual progress bar."""
    if max_val <= 0:
        return "[░░░░░░░░░░░░░░░░░░░░] 0.0%"
    percentage = min(100, (current / max_val) * 100)
    filled = int(percentage / 5)
    empty = 20 - filled
    return f"[{'█' * filled}{'░' * empty}] {percentage:.1f}%"

# Master View with 25 House Buttons (max limit)
class LandsraadView(discord.ui.View):
    def __init__(self, message=None):
        super().__init__(timeout=None)  # Persistent view
        self.message = message  # Store reference to the message this view is attached to
        self.create_house_buttons()
        # Note: Cannot add refresh button due to Discord's 25 component limit
        # Users should use /refresh_panel command instead
    
    def create_house_buttons(self):
        """Create 25 house buttons in a 5x5 grid."""
        houses = get_all_houses()
        
        # Debug: Check alliance values
        debug_mode = False  # Set to True to enable debug output
        if debug_mode:
            for house in houses[:5]:  # Just check first 5
                alliance = house[10] if len(house) > 10 else None
                print(f"DEBUG: House {house[1]} has alliance: '{alliance}'")
        
        # Ensure we only process exactly 25 houses
        for i, house_data in enumerate(houses[:25]):
            row = min(i // 5, 4)  # Cap row at 4 (0-4 are valid)
            button = self.create_house_button(house_data, row)
            self.add_item(button)
    
    def create_house_button(self, house_data: tuple, row: int) -> discord.ui.Button:
        """Create a single house button with appropriate styling."""
        # Unpack relevant data with safe defaults
        house_id = house_data[0]
        name = house_data[1]
        quest = house_data[2] if len(house_data) > 2 else "Unknown"
        current = house_data[3] if len(house_data) > 3 else 0
        goal = house_data[4] if len(house_data) > 4 else GOAL_AMOUNT
        ppd = house_data[5] if len(house_data) > 5 else 1
        is_locked = house_data[6] if len(house_data) > 6 else True
        completed_by = house_data[7] if len(house_data) > 7 else None
        notes = house_data[8] if len(house_data) > 8 else None
        desert_location = house_data[9] if len(house_data) > 9 else None
        alliance = house_data[10] if len(house_data) > 10 else None
        
        # Determine button style and emoji - STRICT CHECKING
        if is_locked:
            style = discord.ButtonStyle.secondary
            emoji = "🔒"
            label = name
        elif alliance == ATREIDES:  # Exact match
            style = discord.ButtonStyle.success  # Green
            emoji = "🟢"
            label = name
        elif alliance == HARKONNEN:  # Exact match
            style = discord.ButtonStyle.danger  # Red
            emoji = "🔴"
            label = name
        else:
            # In progress, not yet claimed or invalid alliance
            style = discord.ButtonStyle.primary  # Blue
            emoji = "🔓"
            label = name
        
        button = discord.ui.Button(
            style=style,
            emoji=emoji,
            label=label,
            custom_id=f"house_{name}",
            row=row
        )
        
        button.callback = self.house_button_callback
        return button
    
    async def house_button_callback(self, interaction: discord.Interaction):
        """Handle house button clicks - now shows action menu."""
        house_name = interaction.data['custom_id'].replace('house_', '')
        house_data = get_house_data(house_name)
        
        if not house_data:
            await interaction.response.send_message(f"❌ House {house_name} not found.", ephemeral=True)
            return
        
        # Show the house action view with buttons
        action_view = HouseActionView(house_name, house_data)
        embed = create_house_info_embed(house_name, house_data)
        
        await interaction.response.send_message(
            f"**House {house_name} Actions**",
            embed=embed,
            view=action_view,
            ephemeral=True
        )

def create_master_embed():
    """Create the master embed showing all houses."""
    houses = get_all_houses()
    
    embed = discord.Embed(
        title="**LANDSRAAD Houses Control Panel**",
        description="Click on a house to unlock, update, or claim it\n💡 Use `/refresh_panel` to refresh the display",
        color=EMBED_COLOR
    )
    
    # Summary statistics - FIXED LOGIC
    unlocked = 0
    claimed = 0
    atreides_count = 0
    harkonnen_count = 0
    
    # Debug mode toggle
    debug_mode = False  # Set to True to see debug output
    
    if debug_mode:
        print("\nDEBUG: Counting houses...")
    
    for h in houses:
        # Safe unpacking with defaults
        name = h[1]
        is_locked = h[6] if len(h) > 6 else True
        alliance = h[10] if len(h) > 10 else None
        current = h[3] if len(h) > 3 else 0
        goal = h[4] if len(h) > 4 else GOAL_AMOUNT
        
        # Count unlocked houses
        if not is_locked:
            unlocked += 1
        
        # Count claimed houses (only those with an alliance)
        if alliance is not None and alliance in [ATREIDES, HARKONNEN]:
            claimed += 1
            if alliance == ATREIDES:
                atreides_count += 1
                if debug_mode:
                    print(f"  {name}: Atreides")
            elif alliance == HARKONNEN:
                harkonnen_count += 1
                if debug_mode:
                    print(f"  {name}: Harkonnen")
    
    if debug_mode:
        print(f"\nDEBUG: Total claimed: {claimed}, Atreides: {atreides_count}, Harkonnen: {harkonnen_count}")
    
    embed.add_field(
        name="📊 Summary",
        value=f"**Unlocked:** {unlocked}/25\n"
              f"**Claimed:** {claimed}/25\n"
              f"**Atreides:** {atreides_count} 🟢\n"
              f"**Harkonnen:** {harkonnen_count} 🔴",
        inline=False
    )
    
    embed.set_footer(text=f"Last refresh: {datetime.now().strftime('%I:%M:%S %p')}")
    
    return embed

# Interactive Map View for Deep Desert
class DeepDesertMapView(discord.ui.View):
    def __init__(self, start_row=0):
        super().__init__(timeout=300)
        self.start_row = start_row
        self.selected_sector = None
        self.create_sector_buttons()
        
    def create_sector_buttons(self):
        """Create a 5x5 grid of sector buttons (25 max Discord limit)."""
        # Show 5 rows at a time due to Discord's 25 button limit
        for row in range(self.start_row, min(self.start_row + 5, 9)):
            for col in range(1, 6):  # Show 5 columns
                if row < 9 and col <= 9:
                    sector_id = f"{chr(65 + row)}{col}"
                    button = self.create_sector_button(sector_id, row - self.start_row)
                    self.add_item(button)
        
        # Add navigation buttons if needed
        if self.start_row > 0:
            prev_button = discord.ui.Button(
                label="◀ Previous",
                style=discord.ButtonStyle.secondary,
                row=4
            )
            prev_button.callback = self.prev_page
            self.add_item(prev_button)
            
        if self.start_row + 5 < 9:
            next_button = discord.ui.Button(
                label="Next ▶",
                style=discord.ButtonStyle.secondary,
                row=4
            )
            next_button.callback = self.next_page
            self.add_item(next_button)
    
    def create_sector_button(self, sector_id: str, row: int) -> discord.ui.Button:
        """Create a button for a sector with appropriate styling."""
        # Get sector data
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        # Check survey status
        cursor.execute('''
        SELECT survey_status FROM deep_desert_sectors WHERE sector_id = ?
        ''', (sector_id,))
        survey_result = cursor.fetchone()
        survey_status = survey_result[0] if survey_result else 'unsurveyed'
        
        # Count POIs in this sector
        cursor.execute('''
        SELECT 
            (SELECT COUNT(*) FROM guild_bases WHERE sector_id = ? AND is_active = 1) as bases,
            (SELECT COUNT(*) FROM spice_locations WHERE sector_id = ? AND is_depleted = 0) as spice,
            (SELECT COUNT(*) FROM landsraad_points WHERE sector_id = ?) as landsraad,
            (SELECT COUNT(*) FROM resource_locations WHERE sector_id = ? AND is_exhausted = 0) as resources
        ''', (sector_id, sector_id, sector_id, sector_id))
        counts = cursor.fetchone()
        total_pois = sum(counts) if counts else 0
        
        conn.close()
        
        # Determine button style and emoji
        if survey_status == 'unsurveyed':
            style = discord.ButtonStyle.secondary
            emoji = "❓"
        elif survey_status == 'partial':
            style = discord.ButtonStyle.primary
            emoji = "🔍"
        else:  # complete
            style = discord.ButtonStyle.success
            emoji = "✅"
        
        # Add POI count to label if any exist
        label = sector_id
        if total_pois > 0:
            label = f"{sector_id} ({total_pois})"
        
        button = discord.ui.Button(
            style=style,
            emoji=emoji,
            label=label,
            custom_id=f"sector_{sector_id}",
            row=row % 5
        )
        button.callback = self.sector_callback
        return button
    
    async def sector_callback(self, interaction: discord.Interaction):
        """Handle sector button clicks."""
        sector_id = interaction.data['custom_id'].replace('sector_', '')
        self.selected_sector = sector_id
        
        # Show sector detail view
        detail_view = SectorDetailView(sector_id)
        embed = create_sector_embed(sector_id)
        
        await interaction.response.send_message(
            f"**Sector {sector_id} Details**",
            embed=embed,
            view=detail_view,
            ephemeral=True
        )
    
    async def next_page(self, interaction: discord.Interaction):
        """Go to next page of sectors."""
        self.start_row = min(self.start_row + 5, 4)  # Max start row is 4 (shows E-I)
        self.clear_items()
        self.create_sector_buttons()
        
        embed = create_map_overview_embed(self.start_row)
        await interaction.response.edit_message(embed=embed, view=self)
    
    async def prev_page(self, interaction: discord.Interaction):
        """Go to previous page of sectors."""
        self.start_row = max(self.start_row - 5, 0)
        self.clear_items()
        self.create_sector_buttons()
        
        embed = create_map_overview_embed(self.start_row)
        await interaction.response.edit_message(embed=embed, view=self)

# Sector Detail View
class SectorDetailView(discord.ui.View):
    def __init__(self, sector_id: str):
        super().__init__(timeout=300)
        self.sector_id = sector_id
        
        # Add location type buttons
        self.add_item(discord.ui.Button(
            label="Add Guild Base",
            emoji="🏰",
            style=discord.ButtonStyle.primary,
            custom_id="add_base"
        ))
        
        self.add_item(discord.ui.Button(
            label="Add Spice Location",
            emoji="🟨",
            style=discord.ButtonStyle.primary,
            custom_id="add_spice"
        ))
        
        self.add_item(discord.ui.Button(
            label="Add Landsraad Point",
            emoji="🏛️",
            style=discord.ButtonStyle.primary,
            custom_id="add_landsraad"
        ))
        
        self.add_item(discord.ui.Button(
            label="Add Resource",
            emoji="💎",
            style=discord.ButtonStyle.primary,
            custom_id="add_resource"
        ))
        
        self.add_item(discord.ui.Button(
            label="Mark Surveyed",
            emoji="✅",
            style=discord.ButtonStyle.success,
            custom_id="mark_surveyed"
        ))
        
        # Set callbacks
        for item in self.children:
            if isinstance(item, discord.ui.Button):
                if item.custom_id == "add_base":
                    item.callback = self.add_base_callback
                elif item.custom_id == "add_spice":
                    item.callback = self.add_spice_callback
                elif item.custom_id == "add_landsraad":
                    item.callback = self.add_landsraad_callback
                elif item.custom_id == "add_resource":
                    item.callback = self.add_resource_callback
                elif item.custom_id == "mark_surveyed":
                    item.callback = self.mark_surveyed_callback
    
    async def add_base_callback(self, interaction: discord.Interaction):
        """Show modal for adding a guild base."""
        modal = AddGuildBaseModal(self.sector_id)
        await interaction.response.send_modal(modal)
    
    async def add_spice_callback(self, interaction: discord.Interaction):
        """Show modal for adding a spice location."""
        modal = AddSpiceModal(self.sector_id)
        await interaction.response.send_modal(modal)
    
    async def add_landsraad_callback(self, interaction: discord.Interaction):
        """Show modal for adding a Landsraad point."""
        modal = AddLandsraadModal(self.sector_id)
        await interaction.response.send_modal(modal)
    
    async def add_resource_callback(self, interaction: discord.Interaction):
        """Show modal for adding a resource location."""
        modal = AddResourceModal(self.sector_id)
        await interaction.response.send_modal(modal)
    
    async def mark_surveyed_callback(self, interaction: discord.Interaction):
        """Mark sector as fully surveyed."""
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        cursor.execute('''
        UPDATE deep_desert_sectors 
        SET survey_status = 'complete', 
            last_surveyed = CURRENT_TIMESTAMP,
            surveyed_by = ?
        WHERE sector_id = ?
        ''', (str(interaction.user), self.sector_id))
        
        conn.commit()
        conn.close()
        
        await interaction.response.send_message(
            f"✅ Sector {self.sector_id} marked as fully surveyed!",
            ephemeral=True
        )

# Modals for adding locations
class AddGuildBaseModal(discord.ui.Modal):
    def __init__(self, sector_id: str):
        super().__init__(title=f"Add Guild Base in Sector {sector_id}")
        self.sector_id = sector_id
        
        self.guild_name = discord.ui.TextInput(
            label="Guild Name",
            placeholder="Enter guild name",
            default="Cold Breakfast Militia",
            required=True
        )
        self.add_item(self.guild_name)
        
        self.base_type = discord.ui.TextInput(
            label="Base Type",
            placeholder="main/outpost/temporary",
            required=True
        )
        self.add_item(self.base_type)
        
        self.alliance = discord.ui.TextInput(
            label="Alliance",
            placeholder="Atreides/Harkonnen/Independent",
            required=False
        )
        self.add_item(self.alliance)
        
        self.coordinates = discord.ui.TextInput(
            label="Section in sector (keypad 1-9)",
            placeholder="7=top-left, 9=top-right, 1=bottom-left, 3=bottom-right",
            required=False
        )
        self.add_item(self.coordinates)
        
        self.notes = discord.ui.TextInput(
            label="Notes",
            placeholder="Additional information",
            style=discord.TextStyle.paragraph,
            required=False
        )
        self.add_item(self.notes)
    
    async def on_submit(self, interaction: discord.Interaction):
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            cursor.execute('''
            INSERT INTO guild_bases (guild_name, sector_id, base_type, alliance, coordinates, discovered_by, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                self.guild_name.value,
                self.sector_id,
                self.base_type.value.lower(),
                self.alliance.value if self.alliance.value else None,
                self.coordinates.value if self.coordinates.value else None,
                str(interaction.user),
                self.notes.value if self.notes.value else None
            ))
            
            conn.commit()
        
        await interaction.response.send_message(
            f"🏰 Guild base '{self.guild_name.value}' added to sector {self.sector_id}!",
            ephemeral=True
        )
        
        # Update location reports
        await update_location_reports(interaction.client, interaction.guild_id)

class AddSpiceModal(discord.ui.Modal):
    def __init__(self, sector_id: str):
        super().__init__(title=f"Add Spice Location in Sector {sector_id}")
        self.sector_id = sector_id
        
        self.size = discord.ui.TextInput(
            label="Size",
            placeholder="Small/Medium/Large",
            required=True
        )
        self.add_item(self.size)
        
        self.estimated_yield = discord.ui.TextInput(
            label="Estimated Spice % Remaining",
            placeholder="Percentage of spice remaining (0-100)",
            required=False
        )
        self.add_item(self.estimated_yield)
        
        self.coordinates = discord.ui.TextInput(
            label="Section in sector (keypad 1-9)",
            placeholder="7=top-left, 9=top-right, 1=bottom-left, 3=bottom-right",
            required=False
        )
        self.add_item(self.coordinates)
        
        self.notes = discord.ui.TextInput(
            label="Notes",
            placeholder="Additional information",
            style=discord.TextStyle.paragraph,
            required=False
        )
        self.add_item(self.notes)
    
    async def on_submit(self, interaction: discord.Interaction):
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            estimated_yield = None
            if self.estimated_yield.value:
                try:
                    estimated_yield = int(self.estimated_yield.value)
                except ValueError:
                    pass
            
            cursor.execute('''
            INSERT INTO spice_locations (sector_id, spice_type, size, estimated_yield, coordinates, discovered_by, notes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ''', (
                self.sector_id,
                'field',  # Default spice type since we removed the field
                self.size.value.lower(),
                estimated_yield,
                self.coordinates.value if self.coordinates.value else None,
                str(interaction.user),
                self.notes.value if self.notes.value else None
            ))
            
            conn.commit()
        
        await interaction.response.send_message(
            f"🟨 Spice location ({self.size.value}) added to sector {self.sector_id}!",
            ephemeral=True
        )
        
        # Update location reports
        await update_location_reports(interaction.client, interaction.guild_id)

class AddLandsraadModal(discord.ui.Modal):
    def __init__(self, sector_id: str):
        super().__init__(title=f"Add Landsraad Point in Sector {sector_id}")
        self.sector_id = sector_id
        
        self.point_name = discord.ui.TextInput(
            label="House Name",
            placeholder="Name of the Landsraad house",
            required=True
        )
        self.add_item(self.point_name)
        
        self.controller = discord.ui.TextInput(
            label="Section in sector (keypad 1-9)",
            placeholder="7=top-left, 9=top-right, 1=bottom-left, 3=bottom-right",
            required=False
        )
        self.add_item(self.controller)
        
        self.tier = discord.ui.TextInput(
            label="Tier (1-3) - Optional",
            placeholder="1 = low, 2 = medium, 3 = high importance",
            required=False
        )
        self.add_item(self.tier)
        
        self.defense_rating = discord.ui.TextInput(
            label="Defense Rating (1-10)",
            placeholder="How well defended (1-10)",
            required=False
        )
        self.add_item(self.defense_rating)
        
        self.notes = discord.ui.TextInput(
            label="Notes",
            placeholder="Additional information",
            style=discord.TextStyle.paragraph,
            required=False
        )
        self.add_item(self.notes)
    
    async def on_submit(self, interaction: discord.Interaction):
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        tier = None
        defense = None
        
        if self.tier.value:
            try:
                tier = int(self.tier.value)
                tier = max(1, min(3, tier))  # Clamp to 1-3
            except ValueError:
                pass
        
        if self.defense_rating.value:
            try:
                defense = int(self.defense_rating.value)
                defense = max(1, min(10, defense))  # Clamp to 1-10
            except ValueError:
                pass
        
        cursor.execute('''
        INSERT INTO landsraad_points (sector_id, point_name, coordinates, tier, defense_rating, captured_by, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            self.sector_id,
            self.point_name.value,
            self.controller.value if self.controller.value else None,  # Using controller field for coordinates
            tier,
            defense,
            str(interaction.user),
            self.notes.value if self.notes.value else None
        ))
        
        conn.commit()
        conn.close()
        
        tier_msg = f" (Tier {tier})" if tier else ""
        await interaction.response.send_message(
            f"🏛️ Landsraad house '{self.point_name.value}'{tier_msg} added to sector {self.sector_id}!",
            ephemeral=True
        )
        
        # Update location reports
        await update_location_reports(interaction.client, interaction.guild_id)

class AddResourceModal(discord.ui.Modal):
    def __init__(self, sector_id: str):
        super().__init__(title=f"Add Resource in Sector {sector_id}")
        self.sector_id = sector_id
        
        self.resource_type = discord.ui.TextInput(
            label="Resource Type",
            placeholder="Titanium/Stravidium/Aluminum/ETC",
            required=True
        )
        self.add_item(self.resource_type)
        
        self.concentration = discord.ui.TextInput(
            label="Concentration",
            placeholder="Tier 1/Tier 2/Tier 3",
            required=True
        )
        self.add_item(self.concentration)
        
        self.coordinates = discord.ui.TextInput(
            label="Section in sector (keypad 1-9)",
            placeholder="7=top-left, 9=top-right, 1=bottom-left, 3=bottom-right",
            required=False
        )
        self.add_item(self.coordinates)
        
        self.notes = discord.ui.TextInput(
            label="Notes",
            placeholder="Additional information",
            style=discord.TextStyle.paragraph,
            required=False
        )
        self.add_item(self.notes)
    
    async def on_submit(self, interaction: discord.Interaction):
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        cursor.execute('''
        INSERT INTO resource_locations (sector_id, resource_type, concentration, extraction_difficulty, coordinates, discovered_by, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ''', (
            self.sector_id,
            self.resource_type.value.lower(),
            self.concentration.value.lower(),
            None,  # No extraction difficulty anymore
            self.coordinates.value if self.coordinates.value else None,
            str(interaction.user),
            self.notes.value if self.notes.value else None
        ))
        
        conn.commit()
        conn.close()
        
        await interaction.response.send_message(
            f"💎 Resource '{self.resource_type.value}' ({self.concentration.value} concentration) added to sector {self.sector_id}!",
            ephemeral=True
        )
        
        # Update location reports
        await update_location_reports(interaction.client, interaction.guild_id)

# Helper functions
def create_sector_embed(sector_id: str) -> discord.Embed:
    """Create an embed showing all information for a sector."""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Get sector info
    cursor.execute('SELECT * FROM deep_desert_sectors WHERE sector_id = ?', (sector_id,))
    sector = cursor.fetchone()
    
    if not sector:
        conn.close()
        return discord.Embed(title=f"Sector {sector_id}", description="No data available", color=0xFF0000)
    
    embed = discord.Embed(
        title=f"Sector {sector_id}",
        color=0xD4AF37
    )
    
    # Survey status
    survey_status = sector[3]
    surveyed_by = sector[5]
    last_surveyed = sector[4]
    
    status_emoji = {"unsurveyed": "❓", "partial": "🔍", "complete": "✅"}.get(survey_status, "❓")
    embed.add_field(
        name="Survey Status",
        value=f"{status_emoji} {survey_status.title()}" + 
              (f"\nBy: {surveyed_by}" if surveyed_by else "") +
              (f"\nDate: {last_surveyed}" if last_surveyed else ""),
        inline=False
    )
    
    # Guild bases
    cursor.execute('''
    SELECT guild_name, base_type, alliance FROM guild_bases 
    WHERE sector_id = ? AND is_active = 1
    ''', (sector_id,))
    bases = cursor.fetchall()
    
    if bases:
        base_text = "\n".join([f"• {b[0]} ({b[1]}) - {b[2] or 'Independent'}" for b in bases])
        embed.add_field(name="🏰 Guild Bases", value=base_text, inline=False)
    
    # Spice locations
    cursor.execute('''
    SELECT spice_type, size, estimated_yield FROM spice_locations 
    WHERE sector_id = ? AND is_depleted = 0
    ''', (sector_id,))
    spice = cursor.fetchall()
    
    if spice:
        spice_text = "\n".join([f"• {s[0].title()} ({s[1]}) - Yield: {s[2] or 'Unknown'}" for s in spice])
        embed.add_field(name="🟨 Spice Locations", value=spice_text, inline=False)
    
    # Landsraad points
    cursor.execute('''
    SELECT point_name, current_controller, tier FROM landsraad_points 
    WHERE sector_id = ?
    ''', (sector_id,))
    landsraad = cursor.fetchall()
    
    if landsraad:
        landsraad_text = "\n".join([f"• {l[0] or 'Unnamed'} - Controller: {l[1] or 'None'} (Tier {l[2]})" for l in landsraad])
        embed.add_field(name="🏛️ Landsraad Points", value=landsraad_text, inline=False)
    
    # Resources
    cursor.execute('''
    SELECT resource_type, concentration FROM resource_locations 
    WHERE sector_id = ? AND is_exhausted = 0
    ''', (sector_id,))
    resources = cursor.fetchall()
    
    if resources:
        resource_text = "\n".join([f"• {r[0].title()} ({r[1]} concentration)" for r in resources])
        embed.add_field(name="💎 Resources", value=resource_text, inline=False)
    
    conn.close()
    return embed

def create_map_overview_embed(start_row: int = 0) -> discord.Embed:
    """Create an embed showing map overview statistics."""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Get survey statistics
    cursor.execute('''
    SELECT 
        COUNT(CASE WHEN survey_status = 'complete' THEN 1 END) as complete,
        COUNT(CASE WHEN survey_status = 'partial' THEN 1 END) as partial,
        COUNT(CASE WHEN survey_status = 'unsurveyed' THEN 1 END) as unsurveyed
    FROM deep_desert_sectors
    ''')
    survey_stats = cursor.fetchone()
    
    # Get POI counts
    cursor.execute('''
    SELECT 
        (SELECT COUNT(*) FROM guild_bases WHERE is_active = 1) as bases,
        (SELECT COUNT(*) FROM spice_locations WHERE is_depleted = 0) as spice,
        (SELECT COUNT(*) FROM landsraad_points) as landsraad,
        (SELECT COUNT(*) FROM resource_locations WHERE is_exhausted = 0) as resources
    ''')
    poi_stats = cursor.fetchone()
    
    conn.close()
    
    embed = discord.Embed(
        title="🗺️ **Deep Desert Map Overview**",
        description=f"Showing rows {chr(65 + start_row)}-{chr(69 + start_row)} • Click a sector for details",
        color=0xD4AF37
    )
    
    embed.add_field(
        name="📊 Survey Progress",
        value=f"✅ Complete: {survey_stats[0]}/81\n"
              f"🔍 Partial: {survey_stats[1]}/81\n"
              f"❓ Unsurveyed: {survey_stats[2]}/81",
        inline=True
    )
    
    embed.add_field(
        name="📍 Points of Interest",
        value=f"🏰 Guild Bases: {poi_stats[0]}\n"
              f"🟨 Spice Locations: {poi_stats[1]}\n"
              f"🏛️ Landsraad Points: {poi_stats[2]}\n"
              f"💎 Resources: {poi_stats[3]}",
        inline=True
    )
    
    embed.add_field(
        name="🗺️ Grid Reference",
        value="Each sector represents ~6.2 km²\n"
              "Total area: ~500 km²\n"
              "Use navigation buttons for more sectors",
        inline=False
    )
    
    embed.set_footer(text="Data resets weekly with Coriolis Storm • Use /deepdesert to access")
    
    return embed

# Helper function to save configuration
def save_bot_config():
    """Save bot configuration to a JSON file."""
    config = {
        'schedule_channel_id': SCHEDULE_CHANNEL_ID,
        'last_schedule_message_id': last_schedule_message_id,
        'last_schedule_channel_id': last_schedule_channel_id
    }
    
    os.makedirs('data', exist_ok=True)
    with open('data/bot_config.json', 'w') as f:
        json.dump(config, f)

def load_bot_config():
    """Load bot configuration from JSON file."""
    global SCHEDULE_CHANNEL_ID, last_schedule_message_id, last_schedule_channel_id
    
    try:
        with open('data/bot_config.json', 'r') as f:
            config = json.load(f)
            SCHEDULE_CHANNEL_ID = config.get('schedule_channel_id')
            last_schedule_message_id = config.get('last_schedule_message_id')
            last_schedule_channel_id = config.get('last_schedule_channel_id')
    except FileNotFoundError:
        pass

# Location Report Functions
def generate_guild_bases_report() -> discord.Embed:
    """Generate a report of all guild bases organized by sector."""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT gb.*, dds.row_letter, dds.col_number
        FROM guild_bases gb
        JOIN deep_desert_sectors dds ON gb.sector_id = dds.sector_id
        WHERE gb.is_active = 1
        ORDER BY dds.row_letter, dds.col_number
        ''')
        
        bases = cursor.fetchall()
    
    embed = discord.Embed(
        title="🏰 **Guild Base Locations**",
        description="All known guild bases in the Deep Desert",
        color=0xD4AF37
    )
    
    if not bases:
        embed.add_field(name="No bases found", value="No guild bases have been discovered yet.", inline=False)
    else:
        current_sector = None
        sector_content = []
        
        for base in bases:
            sector_id = base[2]  # sector_id
            if current_sector != sector_id:
                if current_sector and sector_content:
                    embed.add_field(
                        name=f"**Sector {current_sector}**",
                        value="\n".join(sector_content),
                        inline=False
                    )
                current_sector = sector_id
                sector_content = []
            
            guild_name = base[1]
            base_type = base[4] if base[4] else "unknown"
            alliance = base[5] if base[5] else "Independent"
            coordinates = f" - Section {base[3]}" if base[3] else ""
            
            sector_content.append(f"• **{guild_name}** ({alliance}) - {base_type}{coordinates}")
        
        # Add last sector
        if current_sector and sector_content:
            embed.add_field(
                name=f"**Sector {current_sector}**",
                value="\n".join(sector_content),
                inline=False
            )
    
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    return embed

def generate_spice_locations_report() -> discord.Embed:
    """Generate a report of all spice locations organized by sector."""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT sl.*, dds.row_letter, dds.col_number
    FROM spice_locations sl
    JOIN deep_desert_sectors dds ON sl.sector_id = dds.sector_id
    WHERE sl.is_depleted = 0
    ORDER BY dds.row_letter, dds.col_number
    ''')
    
    spice_locs = cursor.fetchall()
    conn.close()
    
    embed = discord.Embed(
        title="🟨 **Spice Locations**",
        description="All known spice deposits in the Deep Desert",
        color=0xFFD700
    )
    
    if not spice_locs:
        embed.add_field(name="No spice found", value="No spice locations have been discovered yet.", inline=False)
    else:
        current_sector = None
        sector_content = []
        
        for spice in spice_locs:
            sector_id = spice[1]  # sector_id
            if current_sector != sector_id:
                if current_sector and sector_content:
                    embed.add_field(
                        name=f"**Sector {current_sector}**",
                        value="\n".join(sector_content),
                        inline=False
                    )
                current_sector = sector_id
                sector_content = []
            
            size = spice[3] if spice[3] else "unknown"
            coordinates = f" - Section {spice[4]}" if spice[4] else ""
            yield_info = f" ({spice[10]}% remaining)" if spice[10] else ""
            
            sector_content.append(f"• **{size.capitalize()} spice**{coordinates}{yield_info}")
        
        # Add last sector
        if current_sector and sector_content:
            embed.add_field(
                name=f"**Sector {current_sector}**",
                value="\n".join(sector_content),
                inline=False
            )
    
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    return embed

def generate_control_points_report() -> discord.Embed:
    """Generate a report of all Landsraad control points organized by sector."""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT lp.*, dds.row_letter, dds.col_number
    FROM landsraad_points lp
    JOIN deep_desert_sectors dds ON lp.sector_id = dds.sector_id
    ORDER BY dds.row_letter, dds.col_number
    ''')
    
    points = cursor.fetchall()
    conn.close()
    
    embed = discord.Embed(
        title="🏛️ **Landsraad Control Points**",
        description="All known Landsraad houses in the Deep Desert",
        color=0x9932CC
    )
    
    if not points:
        embed.add_field(name="No points found", value="No Landsraad control points have been discovered yet.", inline=False)
    else:
        current_sector = None
        sector_content = []
        
        for point in points:
            sector_id = point[1]  # sector_id
            if current_sector != sector_id:
                if current_sector and sector_content:
                    embed.add_field(
                        name=f"**Sector {current_sector}**",
                        value="\n".join(sector_content),
                        inline=False
                    )
                current_sector = sector_id
                sector_content = []
            
            house_name = point[2]
            coordinates = f" - Section {point[4]}" if point[4] else ""
            tier = f" (Tier {point[5]})" if point[5] else ""
            
            sector_content.append(f"• **House {house_name}**{tier}{coordinates}")
        
        # Add last sector
        if current_sector and sector_content:
            embed.add_field(
                name=f"**Sector {current_sector}**",
                value="\n".join(sector_content),
                inline=False
            )
    
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    return embed

def generate_resource_locations_report() -> discord.Embed:
    """Generate a report of all resource locations organized by sector."""
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    cursor.execute('''
    SELECT rl.*, dds.row_letter, dds.col_number
    FROM resource_locations rl
    JOIN deep_desert_sectors dds ON rl.sector_id = dds.sector_id
    WHERE rl.is_exhausted = 0
    ORDER BY dds.row_letter, dds.col_number
    ''')
    
    resources = cursor.fetchall()
    conn.close()
    
    embed = discord.Embed(
        title="💎 **Resource Locations**",
        description="All known resource deposits in the Deep Desert",
        color=0x00CED1
    )
    
    if not resources:
        embed.add_field(name="No resources found", value="No resource locations have been discovered yet.", inline=False)
    else:
        current_sector = None
        sector_content = []
        
        for resource in resources:
            sector_id = resource[1]  # sector_id
            if current_sector != sector_id:
                if current_sector and sector_content:
                    embed.add_field(
                        name=f"**Sector {current_sector}**",
                        value="\n".join(sector_content),
                        inline=False
                    )
                current_sector = sector_id
                sector_content = []
            
            resource_type = resource[2]
            concentration = resource[3] if resource[3] else "unknown"
            coordinates = f" - Section {resource[4]}" if resource[4] else ""
            
            sector_content.append(f"• **{resource_type.capitalize()}** ({concentration}){coordinates}")
        
        # Add last sector
        if current_sector and sector_content:
            embed.add_field(
                name=f"**Sector {current_sector}**",
                value="\n".join(sector_content),
                inline=False
            )
    
    embed.set_footer(text=f"Last updated: {datetime.now().strftime('%Y-%m-%d %I:%M %p')}")
    return embed

async def update_location_reports(bot, guild_id: int):
    """Update all location report channels with latest data."""
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # Get all configured channels
        cursor.execute('SELECT config_name, channel_id, message_id FROM channel_config WHERE guild_id = ?', (str(guild_id),))
        configs = cursor.fetchall()
    
    guild = bot.get_guild(guild_id)
    if not guild:
        return
    
    for config_name, channel_id, message_id in configs:
        if not channel_id:
            continue
            
        channel = guild.get_channel(int(channel_id))
        if not channel:
            continue
        
        # Generate the appropriate report
        if config_name == 'base_locations':
            embed = generate_guild_bases_report()
        elif config_name == 'spice_locations':
            embed = generate_spice_locations_report()
        elif config_name == 'control_points':
            embed = generate_control_points_report()
        elif config_name == 'resource_locations':
            embed = generate_resource_locations_report()
        else:
            continue
        
        try:
            if message_id:
                # Try to edit existing message
                try:
                    message = await channel.fetch_message(int(message_id))
                    await message.edit(embed=embed)
                except:
                    # Message not found, send new one
                    message = await channel.send(embed=embed)
                    # Update message ID in database
                    with db_manager.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute('UPDATE channel_config SET message_id = ? WHERE config_name = ? AND guild_id = ?',
                                     (str(message.id), config_name, str(guild_id)))
                        conn.commit()
            else:
                # Send new message
                message = await channel.send(embed=embed)
                # Save message ID
                with db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute('UPDATE channel_config SET message_id = ? WHERE config_name = ? AND guild_id = ?',
                                 (str(message.id), config_name, str(guild_id)))
                    conn.commit()
        except Exception as e:
            print(f"Error updating {config_name} channel: {e}")

# Slash Commands
@bot.tree.command(name="landsraad", description="Open the Landsraad control panel")
async def slash_landsraad(interaction: discord.Interaction):
    """Main command to show the Landsraad control panel."""
    view = LandsraadView()
    embed = create_master_embed()
    
    # Send the message and then update the view with message reference
    await interaction.response.send_message(embed=embed, view=view)
    # Get the message that was just sent
    message = await interaction.original_response()
    view.message = message

@bot.tree.command(name="claim_house", description="Quick mark a house as claimed by a faction")
@app_commands.describe(
    house="Name of the house to claim",
    faction="Which faction claimed it (Atreides/Harkonnen)"
)
async def slash_claim_house(interaction: discord.Interaction, house: str, faction: str):
    """Quick command to mark a house as claimed by a faction."""
    # Normalize inputs
    faction = faction.strip().lower()
    
    if faction not in ['atreides', 'harkonnen', 'a', 'h']:
        await interaction.response.send_message(
            "❌ Invalid faction. Use 'Atreides' or 'Harkonnen'.",
            ephemeral=True
        )
        return
    
    # Get house data
    house_data = get_house_data(house)
    if not house_data:
        await interaction.response.send_message(
            f"❌ House '{house}' not found. Make sure to use the exact house name.",
            ephemeral=True
        )
        return
    
    # Determine faction
    if faction in ['atreides', 'a']:
        alliance = ATREIDES
        emoji = "🟢"
    else:
        alliance = HARKONNEN
        emoji = "🔴"
    
    # Claim the house
    success = claim_house_for_alliance(house, alliance, str(interaction.user))
    
    if success:
        await interaction.response.send_message(
            f"{emoji} **House {house} has been claimed by {alliance}!**\n"
            f"Current progress: {house_data[3]:,}/{house_data[4]:,}",
            ephemeral=False
        )
    else:
        await interaction.response.send_message(
            "❌ Failed to claim house.",
            ephemeral=True
        )

@bot.tree.command(name="debug_house", description="Debug house data (Admin only)")
@app_commands.describe(house="Name of the house to debug")
@app_commands.default_permissions(administrator=True)
async def slash_debug_house(interaction: discord.Interaction, house: str):
    """Debug command to check house data."""
    house_data = get_house_data(house)
    if not house_data:
        await interaction.response.send_message(f"❌ House '{house}' not found.", ephemeral=True)
        return
    
    # Create debug info
    debug_info = f"**Debug info for House {house}:**\n```"
    debug_info += f"ID: {house_data[0]}\n"
    debug_info += f"Name: {house_data[1]}\n"
    debug_info += f"Quest: {house_data[2]}\n"
    debug_info += f"Current: {house_data[3]}\n"
    debug_info += f"Goal: {house_data[4]}\n"
    debug_info += f"PPD: {house_data[5]}\n"
    debug_info += f"Is Locked: {house_data[6]}\n"
    debug_info += f"Completed By: {house_data[7]}\n"
    debug_info += f"Notes: {house_data[8]}\n"
    debug_info += f"Desert Location: {house_data[9]}\n"
    debug_info += f"Alliance: '{house_data[10]}' (type: {type(house_data[10])})\n"
    debug_info += f"Deep Desert CP: {house_data[11]}\n"
    debug_info += f"Last Updated: {house_data[12]}\n"
    debug_info += f"Updated By: {house_data[13]}\n"
    debug_info += "```"
    
    await interaction.response.send_message(debug_info, ephemeral=True)

@bot.tree.command(name="fix_database", description="Fix database to ensure only 25 houses (Admin only)")
@app_commands.default_permissions(administrator=True)
async def slash_fix_database(interaction: discord.Interaction):
    """Fix the database to ensure only the 25 Landsraad houses exist."""
    await interaction.response.defer(ephemeral=True)
    
    # Re-populate with exactly 25 houses
    populate_initial_houses()
    
    # Fix any corrupted alliance data
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # First, let's see ALL alliance values INCLUDING usernames stored as alliances
    cursor.execute('SELECT name, alliance FROM houses WHERE alliance IS NOT NULL')
    all_alliances = cursor.fetchall()
    
    # Check what invalid alliances exist (anything that's not Atreides or Harkonnen)
    cursor.execute('''
    SELECT name, alliance FROM houses 
    WHERE alliance IS NOT NULL AND alliance NOT IN (?, ?)
    ''', (ATREIDES, HARKONNEN))
    invalid_alliances = cursor.fetchall()
    
    # AGGRESSIVE FIX: Look for any alliance that contains user-like patterns
    # This will catch cases where usernames were stored instead of alliance names
    cursor.execute('''
    UPDATE houses 
    SET alliance = NULL
    WHERE alliance IS NOT NULL 
    AND alliance NOT IN (?, ?)
    ''', (ATREIDES, HARKONNEN))
    fixed_count = cursor.rowcount
    
    # Also clear completed_by if it has invalid data
    cursor.execute('''
    UPDATE houses 
    SET completed_by = NULL
    WHERE completed_by IS NOT NULL 
    AND completed_by NOT IN (?, ?)
    ''', (ATREIDES, HARKONNEN))
    completed_fixed = cursor.rowcount
    
    # Get final count to confirm
    cursor.execute('SELECT COUNT(*) FROM houses')
    count = cursor.fetchone()[0]
    
    # Get current valid alliances
    cursor.execute('SELECT name, alliance FROM houses WHERE alliance IN (?, ?)', (ATREIDES, HARKONNEN))
    valid_alliances = cursor.fetchall()
    
    conn.commit()
    conn.close()
    
    # Format results
    all_list = "\n".join([f"  {name}: '{alliance}'" for name, alliance in all_alliances[:10]]) if all_alliances else "None"
    if len(all_alliances) > 10:
        all_list += f"\n  ... and {len(all_alliances) - 10} more"
    
    invalid_list = "\n".join([f"  {name}: '{alliance}'" for name, alliance in invalid_alliances]) if invalid_alliances else "None"
    valid_list = "\n".join([f"  {name}: {alliance}" for name, alliance in valid_alliances]) if valid_alliances else "None"
    
    await interaction.followup.send(
        f"✅ **Database deep clean complete!**\n"
        f"• Total houses: {count}\n"
        f"• Invalid alliances fixed: {fixed_count}\n"
        f"• Invalid completed_by fixed: {completed_fixed}\n"
        f"\n**Invalid entries that were fixed:**\n{invalid_list}\n"
        f"\n**Valid alliances remaining:**\n{valid_list}\n"
        f"\n**All entries before fix (first 10):**\n{all_list}",
        ephemeral=True
    )

@bot.tree.command(name="reset_landsraad", description="Reset all houses for new week (Admin only)")
@app_commands.default_permissions(administrator=True)
async def slash_reset_landsraad(interaction: discord.Interaction):
    """Reset all houses for the new week."""
    class ConfirmResetView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
        
        @discord.ui.button(label="Confirm Reset", style=discord.ButtonStyle.danger, emoji="⚠️")
        async def confirm_reset(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            conn = sqlite3.connect(DATABASE)
            cursor = conn.cursor()
            
            # Get completion stats before reset
            cursor.execute('SELECT COUNT(*) FROM houses WHERE alliance IS NOT NULL')
            claimed_count = cursor.fetchone()[0]
            
            # Reset all houses
            cursor.execute('''
            UPDATE houses 
            SET is_locked = 1, 
                current_goal = 0, 
                quest = 'Unknown',
                desert_location = NULL,
                completed_by = NULL,
                alliance = NULL,
                deep_desert_cp = 0,
                last_updated = CURRENT_TIMESTAMP,
                updated_by = ?
            ''', (str(button_interaction.user),))
            
            # Log the reset
            cursor.execute('''
            INSERT INTO reset_log (reset_by, houses_reset, houses_completed) 
            VALUES (?, 25, ?)
            ''', (str(button_interaction.user), claimed_count))
            
            conn.commit()
            conn.close()
            
            await button_interaction.response.edit_message(
                content=f"✅ **Weekly reset complete!**\n"
                        f"• All 25 houses have been locked\n"
                        f"• {claimed_count} houses were claimed last week",
                view=None
            )
        
        @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
        async def cancel_reset(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            await button_interaction.response.edit_message(
                content="❌ Reset cancelled.",
                view=None
            )
    
    await interaction.response.send_message(
        "⚠️ **This will reset ALL 25 houses for the new Landsraad cycle.**\n"
        "This action cannot be undone. Are you sure?",
        view=ConfirmResetView(),
        ephemeral=True
    )

@bot.tree.command(name="set_alliance", description="Manually set a house's alliance (Admin only)")
@app_commands.describe(
    house="Name of the house",
    alliance="Alliance name (Atreides/Harkonnen) or 'none' to clear"
)
@app_commands.default_permissions(administrator=True)
async def slash_set_alliance(interaction: discord.Interaction, house: str, alliance: str):
    """Manually set a house's alliance - for fixing corrupted data."""
    # Get house data
    house_data = get_house_data(house)
    if not house_data:
        await interaction.response.send_message(f"❌ House '{house}' not found.", ephemeral=True)
        return
    
    # Normalize alliance input
    alliance = alliance.strip().lower()
    
    if alliance in ['none', 'null', 'clear', '']:
        # Clear the alliance
        update_house_data(house, 'alliance', None, str(interaction.user))
        await interaction.response.send_message(
            f"✅ Cleared alliance for House {house}",
            ephemeral=True
        )
    elif alliance in ['atreides', 'a']:
        # Set to Atreides
        update_house_data(house, 'alliance', ATREIDES, str(interaction.user))
        await interaction.response.send_message(
            f"🟢 Set House {house} alliance to {ATREIDES}",
            ephemeral=True
        )
    elif alliance in ['harkonnen', 'h']:
        # Set to Harkonnen
        update_house_data(house, 'alliance', HARKONNEN, str(interaction.user))
        await interaction.response.send_message(
            f"🔴 Set House {house} alliance to {HARKONNEN}",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            "❌ Invalid alliance. Use 'Atreides', 'Harkonnen', or 'none'",
            ephemeral=True
        )

@bot.tree.command(name="refresh_panel", description="Force refresh the Landsraad panel")
async def slash_refresh_panel(interaction: discord.Interaction):
    """Force a refresh of the panel by sending a new one."""
    view = LandsraadView()
    embed = create_master_embed()
    
    await interaction.response.send_message(
        content="🔄 **Refreshed panel:**",
        embed=embed, 
        view=view
    )
    
    # Update view reference
    message = await interaction.original_response()
    view.message = message

@bot.tree.command(name="weeklyschedule", description="Show the weekly Dune Awakening schedule")
async def slash_weekly_schedule(interaction: discord.Interaction):
    """Show the weekly schedule with dynamic timestamps."""
    embed = create_schedule_embed()
    
    # Add configuration info for admins
    if interaction.user.guild_permissions.administrator:
        config_info = "\n**Admin Info:**"
        if SCHEDULE_CHANNEL_ID:
            channel = interaction.guild.get_channel(SCHEDULE_CHANNEL_ID)
            if channel:
                config_info += f"\nAuto-post channel: {channel.mention}"
            else:
                config_info += f"\nAuto-post channel: ID {SCHEDULE_CHANNEL_ID} (not in this server)"
        else:
            config_info += "\nNo auto-post channel set. Use `/set_schedule_channel`"
        
        await interaction.response.send_message(embed=embed, content=config_info, ephemeral=True)
    else:
        await interaction.response.send_message(embed=embed)

@bot.tree.command(name="post_schedule", description="Post schedule to a specific channel")
@app_commands.describe(channel="The channel to post the schedule in")
@app_commands.default_permissions(administrator=True)
async def slash_post_schedule(interaction: discord.Interaction, channel: discord.TextChannel):
    """Manually trigger the schedule post to a specific channel."""
    try:
        # Check bot permissions in the channel
        permissions = channel.permissions_for(interaction.guild.me)
        if not permissions.send_messages:
            await interaction.response.send_message(
                f"❌ I don't have permission to send messages in {channel.mention}",
                ephemeral=True
            )
            return
        
        if not permissions.embed_links:
            await interaction.response.send_message(
                f"❌ I don't have permission to send embeds in {channel.mention}",
                ephemeral=True
            )
            return
        
        # Post the schedule
        embed = create_schedule_embed()
        message = await channel.send(
            content="📅 **Weekly Schedule:**",
            embed=embed
        )
        
        # Store message ID for future editing
        global last_schedule_message_id, last_schedule_channel_id
        last_schedule_message_id = message.id
        last_schedule_channel_id = channel.id
        save_bot_config()
        
        await interaction.response.send_message(
            f"✅ Posted weekly schedule to {channel.mention}",
            ephemeral=True
        )
        
    except Exception as e:
        await interaction.response.send_message(
            f"❌ Error posting schedule: {str(e)}",
            ephemeral=True
        )

@bot.tree.command(name="set_schedule_channel", description="Set the default channel for automatic weekly schedules")
@app_commands.describe(channel="The channel for automatic schedule posts")
@app_commands.default_permissions(administrator=True)
async def slash_set_schedule_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Set the default channel for automatic schedule posts."""
    # Store the channel ID in a persistent way
    global SCHEDULE_CHANNEL_ID
    SCHEDULE_CHANNEL_ID = channel.id
    save_bot_config()
    
    # Test permissions
    permissions = channel.permissions_for(interaction.guild.me)
    if not permissions.send_messages or not permissions.embed_links:
        await interaction.response.send_message(
            f"⚠️ Set {channel.mention} as schedule channel, but I don't have full permissions there!\n"
            f"Send Messages: {permissions.send_messages}\n"
            f"Embed Links: {permissions.embed_links}",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"✅ Set {channel.mention} as the default schedule channel!\n"
            f"Automatic posts will go there every Tuesday at 3 AM PST.",
            ephemeral=True
        )

@bot.tree.command(name="list_channels", description="List all channels the bot can see (Debug)")
@app_commands.default_permissions(administrator=True)
async def slash_list_channels(interaction: discord.Interaction):
    """Debug command to list all channels the bot can see."""
    text_channels = []
    for channel in interaction.guild.text_channels:
        permissions = channel.permissions_for(interaction.guild.me)
        can_send = "✅" if permissions.send_messages else "❌"
        can_embed = "✅" if permissions.embed_links else "❌"
        text_channels.append(f"{can_send}{can_embed} {channel.mention} (ID: {channel.id})")
    
    # Split into chunks if too many channels
    channel_list = "\n".join(text_channels[:20])
    if len(text_channels) > 20:
        channel_list += f"\n... and {len(text_channels) - 20} more"
    
    embed = discord.Embed(
        title="📋 Channels I Can See",
        description=channel_list,
        color=0x5865F2
    )
    embed.add_field(
        name="Legend",
        value="First ✅/❌ = Can send messages\nSecond ✅/❌ = Can send embeds",
        inline=False
    )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

@bot.tree.command(name="schedule_test", description="Test schedule calculations (Admin only)")
@app_commands.default_permissions(administrator=True)
async def slash_schedule_test(interaction: discord.Interaction):
    """Test and debug schedule calculations."""
    events = calculate_schedule_events()
    
    debug_info = "**Schedule Debug Info:**\n```"
    debug_info += f"Current Time (PST): {datetime.now(PST).strftime('%Y-%m-%d %I:%M %p %Z')}\n\n"
    
    for event_name, event_time in events.items():
        debug_info += f"{event_name}: {event_time.strftime('%Y-%m-%d %I:%M %p %Z')}\n"
    
    debug_info += f"\nLast Message ID: {last_schedule_message_id}\n"
    debug_info += f"Last Channel ID: {last_schedule_channel_id}\n"
    debug_info += "```"
    
    embed = create_schedule_embed()
    
    await interaction.response.send_message(
        content=debug_info,
        embed=embed,
        ephemeral=True
    )

@bot.tree.command(name="clear_schedule_memory", description="Clear stored schedule message ID (Admin only)")
@app_commands.default_permissions(administrator=True)
async def slash_clear_schedule_memory(interaction: discord.Interaction):
    """Clear the stored schedule message ID."""
    global last_schedule_message_id, last_schedule_channel_id
    
    old_message_id = last_schedule_message_id
    old_channel_id = last_schedule_channel_id
    
    last_schedule_message_id = None
    last_schedule_channel_id = None
    save_bot_config()
    
    await interaction.response.send_message(
        f"✅ Cleared schedule memory\n"
        f"Previous message ID: {old_message_id}\n"
        f"Previous channel ID: {old_channel_id}",
        ephemeral=True
    )

@bot.tree.command(name="export_data", description="Export all house data to CSV (Admin only)")
@app_commands.default_permissions(administrator=True)
async def slash_export_data(interaction: discord.Interaction):
    """Export all house data to CSV."""
    houses = get_all_houses()
    
    output = io.StringIO()
    writer = csv.writer(output)
    
    writer.writerow([
        "House", "Quest", "Current Goal", "Goal", "Points Per Delivery",
        "Status", "Progress %", "Alliance", "Deep Desert CP", "Completed By", "Last Updated By"
    ])
    
    for house in houses:
        # Safe unpacking with defaults
        house_id = house[0]
        name = house[1]
        quest = house[2] if len(house) > 2 else "Unknown"
        current = house[3] if len(house) > 3 else 0
        goal = house[4] if len(house) > 4 else GOAL_AMOUNT
        ppd = house[5] if len(house) > 5 else 1
        is_locked = house[6] if len(house) > 6 else True
        completed_by = house[7] if len(house) > 7 else None
        notes = house[8] if len(house) > 8 else None
        desert_location = house[9] if len(house) > 9 else None
        alliance = house[10] if len(house) > 10 else None
        deep_desert_cp = house[11] if len(house) > 11 else 0
        updated_by = house[13] if len(house) > 13 else "Unknown"
        
        if is_locked:
            status = "Locked"
            progress_pct = "N/A"
        elif alliance:
            status = f"Claimed ({alliance})"
            progress_pct = f"{(current/goal)*100:.1f}%"
        elif current >= goal:
            status = "Completed"
            progress_pct = "100%"
        else:
            status = "In Progress"
            progress_pct = f"{(current/goal)*100:.1f}%"
        
        writer.writerow([
            name, quest, current, goal, ppd, status, progress_pct,
            alliance or "None", deep_desert_cp, completed_by or "None", 
            updated_by
        ])
    
    output.seek(0)
    file = discord.File(
        io.BytesIO(output.getvalue().encode()),
        filename=f"landsraad_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    )
    
    await interaction.response.send_message(
        "📊 Here's your Landsraad data export:",
        file=file,
        ephemeral=True
    )

@bot.tree.command(name="full_reset", description="Completely reset and rebuild the database (Admin only)")
@app_commands.default_permissions(administrator=True)
async def slash_full_reset(interaction: discord.Interaction):
    """Completely reset the database - nuclear option."""
    class ConfirmFullResetView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=30)
        
        @discord.ui.button(label="CONFIRM FULL RESET", style=discord.ButtonStyle.danger, emoji="☢️")
        async def confirm_reset(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            # Drop and recreate all tables
            conn = sqlite3.connect(DATABASE)
            cursor = conn.cursor()
            
            # Drop tables
            cursor.execute('DROP TABLE IF EXISTS houses')
            cursor.execute('DROP TABLE IF EXISTS reset_log')
            cursor.execute('DROP TABLE IF EXISTS contributions')
            cursor.execute('DROP TABLE IF EXISTS deep_desert_sectors')
            cursor.execute('DROP TABLE IF EXISTS guild_bases')
            cursor.execute('DROP TABLE IF EXISTS spice_locations')
            cursor.execute('DROP TABLE IF EXISTS landsraad_points')
            cursor.execute('DROP TABLE IF EXISTS resource_locations')
            
            conn.commit()
            conn.close()
            
            # Reinitialize database
            init_database()
            init_database_locations()
            populate_initial_houses()
            
            await button_interaction.response.edit_message(
                content="☢️ **FULL RESET COMPLETE!**\n"
                        "• All tables dropped and recreated\n"
                        "• 25 fresh houses added\n"
                        "• All data has been wiped clean",
                view=None
            )
        
        @discord.ui.button(label="Cancel", style=discord.ButtonStyle.secondary)
        async def cancel_reset(self, button_interaction: discord.Interaction, button: discord.ui.Button):
            await button_interaction.response.edit_message(
                content="❌ Full reset cancelled.",
                view=None
            )
    
    await interaction.response.send_message(
        "☢️ **WARNING: FULL DATABASE RESET**\n"
        "This will:\n"
        "• Delete ALL data\n"
        "• Drop and recreate all tables\n"
        "• Start completely fresh\n\n"
        "**This CANNOT be undone!**",
        view=ConfirmFullResetView(),
        ephemeral=True
    )

# Deep Desert Commands
@bot.tree.command(name="deepdesert", description="Open the Deep Desert map interface")
async def slash_deepdesert(interaction: discord.Interaction):
    """Main command to show the Deep Desert map."""
    # Initialize location tables if not exists
    init_database_locations()
    
    view = DeepDesertMapView(start_row=0)
    embed = create_map_overview_embed(0)
    
    await interaction.response.send_message(embed=embed, view=view)

@bot.tree.command(name="sector", description="View details for a specific sector")
@app_commands.describe(sector="Sector ID (e.g., A1, B5, I9)")
async def slash_sector(interaction: discord.Interaction, sector: str):
    """Quick command to view a specific sector."""
    sector = sector.upper()
    
    # Validate sector ID
    if len(sector) != 2 or sector[0] not in 'ABCDEFGHI' or sector[1] not in '123456789':
        await interaction.response.send_message(
            "❌ Invalid sector ID. Use format like A1, B5, I9",
            ephemeral=True
        )
        return
    
    embed = create_sector_embed(sector)
    view = SectorDetailView(sector)
    
    await interaction.response.send_message(
        f"**Sector {sector} Details**",
        embed=embed,
        view=view
    )

@bot.tree.command(name="quickadd", description="Quickly add a location to a sector")
@app_commands.describe(
    sector="Sector ID (e.g., A1)",
    location_type="Type: base/spice/landsraad/resource",
    name="Location name or description"
)
async def slash_quickadd(interaction: discord.Interaction, sector: str, location_type: str, name: str):
    """Quick command to add locations without modal."""
    sector = sector.upper()
    location_type = location_type.lower()
    
    # Validate inputs
    if len(sector) != 2 or sector[0] not in 'ABCDEFGHI' or sector[1] not in '123456789':
        await interaction.response.send_message("❌ Invalid sector ID", ephemeral=True)
        return
    
    if location_type not in ['base', 'spice', 'landsraad', 'resource']:
        await interaction.response.send_message("❌ Invalid type. Use: base/spice/landsraad/resource", ephemeral=True)
        return
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    if location_type == 'base':
        cursor.execute('''
        INSERT INTO guild_bases (guild_name, sector_id, base_type, discovered_by)
        VALUES (?, ?, 'unknown', ?)
        ''', (name, sector, str(interaction.user)))
        emoji = "🏰"
    elif location_type == 'spice':
        cursor.execute('''
        INSERT INTO spice_locations (sector_id, spice_type, size, discovered_by, notes)
        VALUES (?, 'unknown', 'unknown', ?, ?)
        ''', (sector, str(interaction.user), name))
        emoji = "🟨"
    elif location_type == 'landsraad':
        cursor.execute('''
        INSERT INTO landsraad_points (sector_id, point_name, discovered_by)
        VALUES (?, ?, ?)
        ''', (sector, name, str(interaction.user)))
        emoji = "🏛️"
    else:  # resource
        cursor.execute('''
        INSERT INTO resource_locations (sector_id, resource_type, concentration, discovered_by)
        VALUES (?, ?, 'unknown', ?)
        ''', (sector, name, str(interaction.user)))
        emoji = "💎"
    
    # Update sector to partial if it was unsurveyed
    cursor.execute('''
    UPDATE deep_desert_sectors 
    SET survey_status = CASE 
        WHEN survey_status = 'unsurveyed' THEN 'partial'
        ELSE survey_status 
    END
    WHERE sector_id = ?
    ''', (sector,))
    
    conn.commit()
    conn.close()
    
    await interaction.response.send_message(
        f"{emoji} Added {location_type} '{name}' to sector {sector}!",
        ephemeral=False
    )

# Location Report Configuration Commands
@bot.tree.command(name="set_base_locations_channel", description="Set the channel for guild base location reports")
@app_commands.describe(channel="The channel where guild base reports will be posted")
async def set_base_locations_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Set the channel for automatic guild base location updates."""
    if not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need Manage Channels permission to use this command.", ephemeral=True)
        return
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Initialize location tables if needed
    init_database_locations()
    
    cursor.execute('''
    INSERT OR REPLACE INTO channel_config (config_name, channel_id, guild_id)
    VALUES (?, ?, ?)
    ''', ('base_locations', str(channel.id), str(interaction.guild_id)))
    
    conn.commit()
    conn.close()
    
    await interaction.response.send_message(f"✅ Guild base locations will be posted to {channel.mention}", ephemeral=True)
    
    # Post initial report
    await update_location_reports(bot, interaction.guild_id)

@bot.tree.command(name="set_spice_locations_channel", description="Set the channel for spice location reports")
@app_commands.describe(channel="The channel where spice reports will be posted")
async def set_spice_locations_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Set the channel for automatic spice location updates."""
    if not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need Manage Channels permission to use this command.", ephemeral=True)
        return
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Initialize location tables if needed
    init_database_locations()
    
    cursor.execute('''
    INSERT OR REPLACE INTO channel_config (config_name, channel_id, guild_id)
    VALUES (?, ?, ?)
    ''', ('spice_locations', str(channel.id), str(interaction.guild_id)))
    
    conn.commit()
    conn.close()
    
    await interaction.response.send_message(f"✅ Spice locations will be posted to {channel.mention}", ephemeral=True)
    
    # Post initial report
    await update_location_reports(bot, interaction.guild_id)

@bot.tree.command(name="set_control_points_channel", description="Set the channel for Landsraad control point reports")
@app_commands.describe(channel="The channel where control point reports will be posted")
async def set_control_points_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Set the channel for automatic control point updates."""
    if not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need Manage Channels permission to use this command.", ephemeral=True)
        return
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Initialize location tables if needed
    init_database_locations()
    
    cursor.execute('''
    INSERT OR REPLACE INTO channel_config (config_name, channel_id, guild_id)
    VALUES (?, ?, ?)
    ''', ('control_points', str(channel.id), str(interaction.guild_id)))
    
    conn.commit()
    conn.close()
    
    await interaction.response.send_message(f"✅ Control points will be posted to {channel.mention}", ephemeral=True)
    
    # Post initial report
    await update_location_reports(bot, interaction.guild_id)

@bot.tree.command(name="set_resource_locations_channel", description="Set the channel for resource location reports")
@app_commands.describe(channel="The channel where resource reports will be posted")
async def set_resource_locations_channel(interaction: discord.Interaction, channel: discord.TextChannel):
    """Set the channel for automatic resource location updates."""
    if not interaction.user.guild_permissions.manage_channels:
        await interaction.response.send_message("❌ You need Manage Channels permission to use this command.", ephemeral=True)
        return
    
    conn = sqlite3.connect(DATABASE)
    cursor = conn.cursor()
    
    # Initialize location tables if needed
    init_database_locations()
    
    cursor.execute('''
    INSERT OR REPLACE INTO channel_config (config_name, channel_id, guild_id)
    VALUES (?, ?, ?)
    ''', ('resource_locations', str(channel.id), str(interaction.guild_id)))
    
    conn.commit()
    conn.close()
    
    await interaction.response.send_message(f"✅ Resource locations will be posted to {channel.mention}", ephemeral=True)
    
    # Post initial report
    await update_location_reports(bot, interaction.guild_id)

@bot.tree.command(name="refresh_location_reports", description="Manually refresh all location report channels")
async def refresh_location_reports(interaction: discord.Interaction):
    """Manually trigger an update of all location report channels."""
    if not interaction.user.guild_permissions.manage_messages:
        await interaction.response.send_message("❌ You need Manage Messages permission to use this command.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        await update_location_reports(bot, interaction.guild_id)
        await interaction.followup.send("✅ All location reports have been refreshed!", ephemeral=True)
    except Exception as e:
        await interaction.followup.send(f"❌ Error refreshing reports: {e}", ephemeral=True)

@bot.tree.command(name="force_sync", description="Force sync commands to this guild (admin only)")
async def force_sync(interaction: discord.Interaction):
    """Force sync all slash commands to this guild for instant updates."""
    if not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("❌ You need Administrator permission to use this command.", ephemeral=True)
        return
    
    await interaction.response.defer(ephemeral=True)
    
    try:
        count = await bot.sync_commands_optimized(interaction.guild_id)
        await interaction.followup.send(
            f"✅ Successfully synced {count} commands to this guild! New commands should appear immediately.\n\n"
            f"**Available commands:**\n"
            f"• `/deepdesert` - Access the Deep Desert map interface\n"
            f"• `/set_base_locations_channel` - Configure guild base reports\n"
            f"• `/set_spice_locations_channel` - Configure spice reports\n"
            f"• `/set_control_points_channel` - Configure control point reports\n"
            f"• `/set_resource_locations_channel` - Configure resource reports\n"
            f"• `/refresh_location_reports` - Manually refresh all reports",
            ephemeral=True
        )
    except Exception as e:
        await interaction.followup.send(f"❌ Error syncing commands: {e}", ephemeral=True)

@bot.tree.command(name="bot_status", description="Check bot status and command sync information")
async def bot_status(interaction: discord.Interaction):
    """Show bot status information including command sync status."""
    embed = discord.Embed(
        title="🤖 **Landsraad Bot Status**",
        color=0x00FF00
    )
    
    # Bot info
    embed.add_field(
        name="**Bot Information**",
        value=f"• Connected to {len(bot.guilds)} guilds\n"
              f"• Commands globally synced: {'✅' if bot.synced else '❌'}\n"
              f"• Guild-specific sync: {'✅' if interaction.guild_id in bot.guild_sync_complete else '❌'}",
        inline=False
    )
    
    # Command availability
    if interaction.guild_id in bot.guild_sync_complete:
        status = "🟢 **All commands available immediately**"
    elif bot.synced:
        status = "🟡 **Commands synced globally** (may take up to 1 hour to appear)"
    else:
        status = "🔴 **Commands not yet synced**"
    
    embed.add_field(
        name="**Command Availability**",
        value=status,
        inline=False
    )
    
    # Quick fix
    if interaction.guild_id not in bot.guild_sync_complete:
        embed.add_field(
            name="**Need commands immediately?**",
            value="Use `/force_sync` (requires Administrator permission) to sync commands to this guild instantly!",
            inline=False
        )
    
    await interaction.response.send_message(embed=embed, ephemeral=True)

# Automatic Schedule Posting Task
@tasks.loop(time=time(hour=3, minute=0, tzinfo=PST))  # 3:00 AM PST daily
async def weekly_schedule_post():
    """Automatically post the weekly schedule on Tuesdays at 3 AM PST."""
    try:
        # Only run on Tuesdays (weekday 1)
        current_time = datetime.now(PST)
        if current_time.weekday() != 1:  # 1 = Tuesday
            return
        
        global last_schedule_message_id, last_schedule_channel_id, SCHEDULE_CHANNEL_ID
        
        # Find the schedule channel
        target_channel = None
        
        # First try to use the saved channel ID
        if SCHEDULE_CHANNEL_ID:
            for guild in bot.guilds:
                target_channel = guild.get_channel(SCHEDULE_CHANNEL_ID)
                if target_channel:
                    break
        
        # If no saved channel or not found, try to find by name
        if not target_channel:
            for guild in bot.guilds:
                for channel in guild.text_channels:
                    if channel.name.lower() in ['weeklyschedule', 'weekly-schedule', 'schedule', 'bot-schedule']:
                        target_channel = channel
                        break
                if target_channel:
                    break
        
        if not target_channel:
            print(f"Warning: Could not find schedule channel for automatic posting")
            return
        
        # Check permissions
        permissions = target_channel.permissions_for(target_channel.guild.me)
        if not permissions.send_messages or not permissions.embed_links:
            print(f"Warning: Missing permissions in {target_channel.name}")
            return
        
        # Try to delete or edit the previous schedule post
        if last_schedule_message_id and last_schedule_channel_id == target_channel.id:
            try:
                old_message = await target_channel.fetch_message(last_schedule_message_id)
                await old_message.delete()
                print(f"Deleted previous schedule post (ID: {last_schedule_message_id})")
            except discord.NotFound:
                print("Previous schedule message was already deleted")
            except discord.HTTPException as e:
                print(f"Could not delete previous schedule message: {e}")
        
        # Post the new schedule
        embed = create_schedule_embed()
        message = await target_channel.send(
            content="🆕 **New Landsraad Term - Weekly Schedule Updated:**",
            embed=embed
        )
        
        # Store the new message ID for next week
        last_schedule_message_id = message.id
        last_schedule_channel_id = target_channel.id
        save_bot_config()
        
        print(f"Automatically posted weekly schedule to #{target_channel.name} in {target_channel.guild.name}")
        
    except Exception as e:
        print(f"Error posting automatic schedule: {e}")

@weekly_schedule_post.before_loop
async def before_weekly_schedule():
    """Wait for bot to be ready before starting the task."""
    await bot.wait_until_ready()

# Events
@bot.event
async def on_ready():
    print(f'{bot.user} has connected to Discord!')
    print(f'Bot is in {len(bot.guilds)} guilds')
    
    # Load saved configuration
    load_bot_config()
    
    # Initialize databases (now happens in setup_hook for faster startup)
    # init_database() and init_database_locations() already called in setup_hook
    populate_initial_houses()
    
    # Check for and fix corrupted data on startup using optimized database manager
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        # Check for invalid alliances
        cursor.execute('''
        SELECT COUNT(*) FROM houses 
        WHERE alliance IS NOT NULL AND alliance NOT IN (?, ?)
        ''', (ATREIDES, HARKONNEN))
        invalid_count = cursor.fetchone()[0]
        
        if invalid_count > 0:
            print(f"WARNING: Found {invalid_count} houses with invalid alliances. Auto-fixing...")
            cursor.execute('''
            UPDATE houses 
            SET alliance = NULL
            WHERE alliance IS NOT NULL AND alliance NOT IN (?, ?)
            ''', (ATREIDES, HARKONNEN))
            conn.commit()
            print(f"Fixed {cursor.rowcount} houses with invalid alliances.")
    
    print('Database initialized with 25 houses.')
    
    # Optimized command syncing
    try:
        # Sync commands globally (may take up to 1 hour to propagate)
        await bot.sync_commands_optimized()
        
        # For immediate testing, sync to each guild individually
        if len(bot.guilds) <= 5:  # Only auto-sync to guilds if bot is in 5 or fewer servers
            for guild in bot.guilds:
                try:
                    await bot.sync_commands_optimized(guild.id)
                    print(f"Synced commands to guild: {guild.name} ({guild.id}) - instant availability")
                except Exception as e:
                    print(f"Failed to sync commands to guild {guild.name}: {e}")
        else:
            print(f"Bot is in {len(bot.guilds)} guilds. Use /force_sync in individual guilds for instant command updates.")
    except Exception as e:
        print(f"Command sync error: {e}")
    
    # Start the weekly schedule posting task
    if not weekly_schedule_post.is_running():
        weekly_schedule_post.start()
        print("Started weekly schedule posting task (Tuesdays 3:00 AM PST)")
    
    await bot.change_presence(
        activity=discord.Activity(
            type=discord.ActivityType.watching,
            name="LANDSRAAD Progress | /landsraad | /force_sync"
        )
    )

# Error handling
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    try:
        if isinstance(error, app_commands.CommandOnCooldown):
            await interaction.response.send_message(
                f"Command on cooldown. Try again in {error.retry_after:.0f} seconds.",
                ephemeral=True
            )
        else:
            await interaction.response.send_message(
                f"An error occurred: {str(error)}",
                ephemeral=True
            )
    except:
        pass

# Run the bot
if __name__ == "__main__":
    if not TOKEN:
        logger.error("Please set DISCORD_BOT_TOKEN environment variable")
        print("Please set DISCORD_BOT_TOKEN environment variable")
    else:
        try:
            logger.info("Starting Landsraad Bot...")
            bot.run(TOKEN, log_handler=None)  # Use our custom logging
        except KeyboardInterrupt:
            logger.info("Bot shutdown requested by user")
            print("Bot shutdown requested...")
        except Exception as e:
            logger.error(f"Bot encountered an error: {e}", exc_info=True)
            print(f"Bot encountered an error: {e}")
        finally:
            # Cleanup resources
            try:
                if weekly_schedule_post.is_running():
                    weekly_schedule_post.stop()
                    logger.info("Stopped weekly schedule task")
                    print("Stopped weekly schedule task")
                
                # Close database connections
                db_manager.close_all()
                logger.info("Closed all database connections")
                print("Closed database connections")
            except Exception as e:
                logger.error(f"Error during cleanup: {e}")
                print(f"Error during cleanup: {e}")